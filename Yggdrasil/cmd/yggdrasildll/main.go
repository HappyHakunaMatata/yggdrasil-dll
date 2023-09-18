package main

// #include "stdio.h"
// #include "stdlib.h"
import "C"

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/text/encoding/unicode"

	"github.com/gologme/log"
	gsyslog "github.com/hashicorp/go-syslog"
	"github.com/hjson/hjson-go"
	"github.com/kardianos/minwinsvc"
	"github.com/mitchellh/mapstructure"

	"github.com/yggdrasil-network/yggdrasil-go/src/address"
	"github.com/yggdrasil-network/yggdrasil-go/src/admin"
	"github.com/yggdrasil-network/yggdrasil-go/src/config"
	"github.com/yggdrasil-network/yggdrasil-go/src/defaults"
	"github.com/yggdrasil-network/yggdrasil-go/src/ipv6rwc"

	"github.com/HappyHakunaMatata/LittleMozzarellaNetwork/Yggdrasil/src/version"
	"github.com/yggdrasil-network/yggdrasil-go/src/core"
	"github.com/yggdrasil-network/yggdrasil-go/src/multicast"
	"github.com/yggdrasil-network/yggdrasil-go/src/tun"
)

type node struct {
	core      *core.Core
	tun       *tun.TunAdapter
	multicast *multicast.Multicast
	admin     *admin.AdminSocket
}

func readConfig(log *log.Logger, useconffile string, normaliseconf bool) *config.NodeConfig {
	// Use a configuration file. If -useconf, the configuration will be read
	// from stdin. If -useconffile, the configuration will be read from the
	// filesystem.
	var conf []byte
	var err error
	if useconffile != "" {
		// Read the file from the filesystem
		conf, err = os.ReadFile(useconffile)
	}
	if err != nil {
		panic(err)
	}
	// If there's a byte order mark - which Windows 10 is now incredibly fond of
	// throwing everywhere when it's converting things into UTF-16 for the hell
	// of it - remove it and decode back down into UTF-8. This is necessary
	// because hjson doesn't know what to do with UTF-16 and will panic
	if bytes.Equal(conf[0:2], []byte{0xFF, 0xFE}) ||
		bytes.Equal(conf[0:2], []byte{0xFE, 0xFF}) {
		utf := unicode.UTF16(unicode.BigEndian, unicode.UseBOM)
		decoder := utf.NewDecoder()
		conf, err = decoder.Bytes(conf)
		if err != nil {
			panic(err)
		}
	}
	// Generate a new configuration - this gives us a set of sane defaults -
	// then parse the configuration we loaded above on top of it. The effect
	// of this is that any configuration item that is missing from the provided
	// configuration will use a sane default.
	cfg := defaults.GenerateConfig()
	var dat map[string]interface{}
	if err := hjson.Unmarshal(conf, &dat); err != nil {
		panic(err)
	}
	// Sanitise the config
	confJson, err := json.Marshal(dat)
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(confJson, &cfg); err != nil {
		panic(err)
	}
	// Overlay our newly mapped configuration onto the autoconf node config that
	// we generated above.
	if err = mapstructure.Decode(dat, &cfg); err != nil {
		panic(err)
	}
	return cfg
}

// Generates a new configuration and returns it in HJSON format. This is used
// with -genconf.
func doGenconf(isjson bool) string {
	cfg := defaults.GenerateConfig()
	var bs []byte
	var err error
	if isjson {
		bs, err = json.MarshalIndent(cfg, "", "  ")
	} else {
		bs, err = hjson.Marshal(cfg)
	}
	if err != nil {
		panic(err)
	}
	return string(bs)
}

func setLogLevel(loglevel string, logger *log.Logger) {
	levels := [...]string{"error", "warn", "info", "debug", "trace"}
	loglevel = strings.ToLower(loglevel)

	contains := func() bool {
		for _, l := range levels {
			if l == loglevel {
				return true
			}
		}
		return false
	}

	if !contains() { // set default log level
		logger.Infoln("Loglevel parse failed. Set default level(info)")
		loglevel = "info"
	}
	fmt.Printf("Log level %s is set\n", loglevel)
	for _, l := range levels {
		logger.EnableLevel(l)
		if l == loglevel {
			break
		}
	}
}

type YggArgs struct {
	genconf       bool
	normaliseconf bool
	confjson      bool
	autoconf      bool
	buildName     bool
	ver           bool
	getaddr       bool
	getsnet       bool
	useconffile   string
	logto         string
	loglevel      string
}

var flagIsSet = false

func getArgs(args YggArgs) YggArgs {
	genconf := flag.Bool("genconf", args.genconf, "print a new config to stdout")
	useconffile := flag.String("useconffile", args.useconffile, "read HJSON/JSON config from specified file path")
	normaliseconf := flag.Bool("normaliseconf", args.normaliseconf, "use in combination with either -useconf or -useconffile, outputs your configuration normalised")
	confjson := flag.Bool("json", args.confjson, "print configuration from -genconf or -normaliseconf as JSON instead of HJSON")
	autoconf := flag.Bool("autoconf", args.autoconf, "automatic mode (dynamic IP, peer with IPv6 neighbors)")
	ver := flag.Bool("version", args.ver, "prints the version of this build")
	buildName := flag.Bool("buildname", args.buildName, "prints the build name")
	logto := flag.String("logto", args.logto, "file path to log to, \"syslog\" or \"stdout\"")
	getaddr := flag.Bool("address", args.getaddr, "returns the IPv6 address as derived from the supplied configuration")
	getsnet := flag.Bool("subnet", args.getsnet, "returns the IPv6 subnet as derived from the supplied configuration")
	loglevel := flag.String("loglevel", args.loglevel, "loglevel to enable")
	flag.Parse()
	return YggArgs{
		genconf:       *genconf,
		useconffile:   *useconffile,
		normaliseconf: *normaliseconf,
		confjson:      *confjson,
		autoconf:      *autoconf,
		ver:           *ver,
		buildName:     *buildName,
		logto:         *logto,
		getaddr:       *getaddr,
		getsnet:       *getsnet,
		loglevel:      *loglevel,
	}
}

// The main function is responsible for configuring and starting Yggdrasil.
//

func run(args YggArgs, ctx context.Context) (err_code int, result *C.char) {

	if logger == nil {
		logger = log.New(os.Stdout, "", log.Flags())
		logger.Warnln("Logging defaulting to stdout")
	}

	var err error

	switch {
	case args.autoconf:
		// Use an autoconf-generated config, this will give us random keys and
		// port numbers, and will use an automatically selected TUN interface.
		cfg = defaults.GenerateConfig()
		fmt.Println("Config file generated successfully")
	}
	// Have we got a working configuration? If we don't then it probably means
	// that neither -autoconf, -useconf or -useconffile were set above. Stop
	// if we don't.
	if cfg == nil {
		return 1, C.CString("Please specify config file")
	}

	fmt.Println("Yggdrsil is starting...")
	n := &node{}

	// Setup the Yggdrasil node itself.
	{
		sk, err := hex.DecodeString(cfg.PrivateKey)
		if err != nil {
			defer func() {
				if r := recover(); r != nil {
					err_code = 1
					fmt.Println(err.Error())
				}
			}()
			panic(err)
		}
		options := []core.SetupOption{
			core.NodeInfo(cfg.NodeInfo),
			core.NodeInfoPrivacy(cfg.NodeInfoPrivacy),
		}
		for _, addr := range cfg.Listen {
			options = append(options, core.ListenAddress(addr))
		}
		for _, peer := range cfg.Peers {
			options = append(options, core.Peer{URI: peer})
		}
		for intf, peers := range cfg.InterfacePeers {
			for _, peer := range peers {
				options = append(options, core.Peer{URI: peer, SourceInterface: intf})
			}
		}
		for _, allowed := range cfg.AllowedPublicKeys {
			k, err := hex.DecodeString(allowed)
			if err != nil {
				defer func() {
					if r := recover(); r != nil {
						err_code = 1
						fmt.Println(err.Error())
					}
				}()
				panic(err)
			}
			options = append(options, core.AllowedPublicKey(k[:]))
		}
		if n.core, err = core.New(sk[:], logger, options...); err != nil {
			defer func() {
				if r := recover(); r != nil {
					err_code = 1
					fmt.Println(err.Error())
				}
			}()
			panic(err)
		}
	}

	//w := &yggdrasildb.DBWriter{}
	// Setup the admin socket.
	{
		options := []admin.SetupOption{
			admin.ListenAddress(cfg.AdminListen),
		}
		if n.admin, err = admin.New(n.core, logger, options...); err != nil {
			defer func() {
				if r := recover(); r != nil {
					err_code = 1
					fmt.Println(err.Error())
				}
			}()
			panic(err)
		}
		if n.admin != nil {
			n.admin.SetupAdminHandlers()
		}

		/*
			if w, err = yggdrasildb.New(n.core, logger); err != nil {
				defer func() {
					if r := recover(); r != nil {
						err_code = 1
						fmt.Println(err.Error())
					}
				}()
				panic(err)
			}*/
	}

	// Setup the multicast module.
	{
		options := []multicast.SetupOption{}
		for _, intf := range cfg.MulticastInterfaces {
			options = append(options, multicast.MulticastInterface{
				Regex:    regexp.MustCompile(intf.Regex),
				Beacon:   intf.Beacon,
				Listen:   intf.Listen,
				Port:     intf.Port,
				Priority: uint8(intf.Priority),
			})
		}
		if n.multicast, err = multicast.New(n.core, logger, options...); err != nil {
			defer func() {
				if r := recover(); r != nil {
					err_code = 1
					fmt.Println(err.Error())
				}
			}()
			panic(err)
		}
		if n.admin != nil && n.multicast != nil {
			n.multicast.SetupAdminHandlers(n.admin)
		}
	}

	// Setup the TUN module.
	{
		options := []tun.SetupOption{
			tun.InterfaceName(cfg.IfName),
			tun.InterfaceMTU(cfg.IfMTU),
		}
		if n.tun, err = tun.New(ipv6rwc.NewReadWriteCloser(n.core), logger, options...); err != nil {
			defer func() {
				if r := recover(); r != nil {
					err_code = 1
					fmt.Println(err.Error())
				}
			}()
			panic(err)
		}
		if n.admin != nil && n.tun != nil {
			n.tun.SetupAdminHandlers(n.admin)
		}
	}

	// Make some nice output that tells us what our IPv6 address and subnet are.
	// This is just logged to stdout for the user.
	address := n.core.Address()
	subnet := n.core.Subnet()
	public := n.core.GetSelf().Key
	logger.Infof("Your public key is %s", hex.EncodeToString(public[:]))
	logger.Infof("Your IPv6 address is %s", address.String())
	logger.Infof("Your IPv6 subnet is %s", subnet.String())

	// Block until we are told to shut down.
	<-ctx.Done()

	// Shut down the node.
	//_ = w.Stop()
	_ = n.admin.Stop()
	_ = n.multicast.Stop()
	_ = n.tun.Stop()
	n.core.Stop()
	return 0, C.CString("Successfully completed")
}

func main() {

}

// Catch interrupts from the operating system to exit gracefully.
var ctx, cancel = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

//export start
func start(autoconf bool) (int, *C.char) {

	args.autoconf = autoconf
	if !flagIsSet {
		flagIsSet = true
		args = getArgs(args)
	}

	// Capture the service being stopped on Windows.
	minwinsvc.SetOnExit(cancel)

	// Start the node, block and then wait for it to shut down.
	var result int
	var message *C.char
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		result, message = run(args, ctx)
	}()
	wg.Wait()
	return result, message
}

var logger *log.Logger
var cfg *config.NodeConfig

//export setup
func setup(genconf int, normaliseconf int, confjson int,
	ver int, getaddr int, getsnet int, buildname int,
	useconffile *C.char, loglevel *C.char, logto *C.char) (int, *C.char) {

	val := GetBool(genconf)
	if val != nil {
		args.genconf = *val
	}
	val = GetBool(normaliseconf)
	if val != nil {
		args.normaliseconf = *val
	}
	val = GetBool(confjson)
	if val != nil {
		args.confjson = *val
	}
	val = GetBool(ver)
	if val != nil {
		args.ver = *val
	}
	val = GetBool(getaddr)
	if val != nil {
		args.getaddr = *val
	}
	val = GetBool(getsnet)
	if val != nil {
		args.getsnet = *val
	}
	val = GetBool(buildname)
	if val != nil {
		args.buildName = *val
	}
	if useconffile != nil {
		args.useconffile = C.GoString(useconffile)
	}
	if loglevel != nil {
		args.loglevel = C.GoString(loglevel)
	}
	if logto != nil {
		args.logto = C.GoString(logto)
	}

	if !flagIsSet {
		flagIsSet = true
		args = getArgs(args)
	}

	switch args.logto {
	case "stdout":
		fmt.Println("stdout is set")
		logger = log.New(os.Stdout, "", log.Flags())
	case "syslog":
		fmt.Println("syslog is set")
		if syslogger, err := gsyslog.NewLogger(gsyslog.LOG_NOTICE, "DAEMON", version.BuildName()); err == nil {
			logger = log.New(syslogger, "", log.Flags())
		}
	default:
		if logfd, err := os.OpenFile(args.logto, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
			logger = log.New(logfd, "", log.Flags())
		}
		fmt.Printf("Path %s is set\n", args.logto)
	}

	if logger == nil {
		logger = log.New(os.Stdout, "", log.Flags())
		logger.Warnln("Logging defaulting to stdout")
	}
	if args.normaliseconf {
		setLogLevel("error", logger)
	} else {
		setLogLevel(args.loglevel, logger)
	}

	var err error
	switch {
	case args.buildName:
		buildName := version.BuildName()
		return 0, C.CString(buildName)
	case args.ver:
		buildVersion := version.BuildVersion()
		return 0, C.CString(buildVersion)
	case args.useconffile != "":
		// Read the configuration from either stdin or from the filesystem
		cfg = readConfig(logger, args.useconffile, args.normaliseconf)
		// If the -normaliseconf option was specified then remarshal the above
		// configuration and print it back to stdout. This lets the user update
		// their configuration file with newly mapped names (like above) or to
		// convert from plain JSON to commented HJSON.
		if args.normaliseconf {
			var bs []byte
			if args.confjson {
				bs, err = json.MarshalIndent(cfg, "", "  ")
			} else {
				bs, err = hjson.Marshal(cfg)
			}
			if err != nil {
				defer func() {
					if r := recover(); r != nil {
						fmt.Println(err.Error())
					}
				}()
				panic(err)
			}
			fmt.Println(string(bs))
		}
		fmt.Printf("Config file %s loaded successfully\n", args.useconffile)
	case args.genconf:
		// Generate a new configuration and print it to stdout.
		cfg := doGenconf(args.confjson)
		f, err := os.Create("yggdrasil.conf")
		if err != nil {
			return 1, C.CString(err.Error())
		}
		defer f.Close()
		_, err2 := f.WriteString(cfg)
		if err2 != nil {
			return 1, C.CString(err2.Error())
		}
		fmt.Printf("Config file %s created successfully\n", f.Name())
		return 0, C.CString(f.Name())
	default:
		// No flags were provided, therefore print the list of flags to stdout.
		fmt.Println("Usage:")
		flag.PrintDefaults()

		if args.getaddr || args.getsnet {
			fmt.Println("\nError: You need to specify some config data using -useconf or -useconffile.")
		}

		return 0, C.CString("Help was provided")
	}

	// Have we been asked for the node address yet? If so, print it and then stop.
	getNodeKey := func() ed25519.PublicKey {
		if pubkey, err := hex.DecodeString(cfg.PrivateKey); err == nil {
			return ed25519.PrivateKey(pubkey).Public().(ed25519.PublicKey)
		}
		return nil
	}
	switch {
	case args.getaddr:
		if key := getNodeKey(); key != nil {
			addr := address.AddrForKey(key)
			ip := net.IP(addr[:])
			return 0, C.CString(ip.String())
		}
		return 1, C.CString("IP not found")
	case args.getsnet:
		if key := getNodeKey(); key != nil {
			snet := address.SubnetForKey(key)
			ipnet := net.IPNet{
				IP:   append(snet[:], 0, 0, 0, 0, 0, 0, 0, 0),
				Mask: net.CIDRMask(len(snet)*8, 128),
			}
			return 0, C.CString(ipnet.String())
		}
		return 1, C.CString("Subnet not found")
	}
	return 0, C.CString("OK")
}

var args = YggArgs{
	genconf:       false,
	normaliseconf: false,
	confjson:      false,
	autoconf:      false,
	ver:           false,
	buildName:     false,
	getaddr:       false,
	getsnet:       false,
	useconffile:   "",
	logto:         "",
	loglevel:      "info",
}

func GetBool(parametr int) *bool {
	switch parametr {
	case 0:
		val := false
		return &val
	case 1:
		val := true
		return &val
	default:
		return nil
	}
}

//export Exit
func Exit() int {
	cancel()
	return 0
}
