package yggdrasildb

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"net"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/yggdrasil-network/yggdrasil-go/src/address"
	"github.com/yggdrasil-network/yggdrasil-go/src/core"
)

type DBWriter struct {
	core *core.Core
	done chan struct{}
	db   *sql.DB
	log  core.Logger
}

var schemas = []string{
	`CREATE TABLE IF NOT EXISTS usage (
		id INTEGER PRIMARY KEY, 
		IPAddress TEXT, PublicKey TEXT, 
		RXBytes INTEGER, TXBytes INTEGER, Uptime REAL, instance_id INTEGER,
		FOREIGN KEY(instance_id) REFERENCES Instance_Sessions(id))`,
	`CREATE TABLE IF NOT EXISTS additional_data (
			id INTEGER PRIMARY KEY, usage_id INTEGER,
			Port INTEGER, Priority INTEGER, Remote TEXT, 
			Coords INTEGER,
			FOREIGN KEY(Coords) REFERENCES Coordinates(id)
			FOREIGN KEY(usage_id) REFERENCES usage(id))`,
	`CREATE TABLE IF NOT EXISTS Sessions (
		id INTEGER PRIMARY KEY, 
		IPAddress TEXT, PublicKey TEXT,
		RXBytes INTEGER, TXBytes INTEGER, Uptime REAL, 
		instance_id INTEGER,
		FOREIGN KEY(instance_id) REFERENCES Instance_Sessions(id))`,
	`CREATE TABLE IF NOT EXISTS Instance_Sessions (
		id INTEGER PRIMARY KEY, 
		Timestamp INTEGER)`,
	`CREATE TABLE IF NOT EXISTS Coordinates (
			id INTEGER PRIMARY KEY, 
			X INTEGER NULL, Y INTEGER NULL, Z INTEGER NULL, 
			T INTEGER NULL, N INTEGER NULL)`,
}

/*`CREATE TRIGGER IF NOT EXISTS update_usage
AFTER UPDATE ON usage
FOR EACH ROW
WHEN NEW.RXBytes != OLD.RXBytes OR NEW.TXBytes != OLD.TXBytes OR NEW.Uptime != OLD.Uptime
BEGIN
	UPDATE usage SET RXBytes = OLD.RXBytes + NEW.RXBytes, TXBytes = OLD.TXBytes + NEW.TXBytes, Uptime = OLD.Uptime + NEW.Uptime WHERE id = OLD.id;
END;`,*/

func initDB() (*sql.DB, error) {
	database, _ := sql.Open("sqlite3", "/Users/main/Projects/yggdrasil-go/src/yggdrasildb/usage.db")
	for _, schema := range schemas {
		_, err := database.Exec(schema)
		if err != nil {
			return nil, err
		}
	}
	return database, nil
}

func AddPeers(w *DBWriter, id_intance int64) {
	peers := w.core.GetPeers()
	for _, peer := range peers {
		addr := address.AddrForKey(peer.Key)
		id_usage, err := w.GetValue("SELECT id FROM usage WHERE IPAddress=? AND PublicKey=? AND instance_id=?",
			net.IP(addr[:]).String(),
			hex.EncodeToString(peer.Key),
			id_intance)
		if err != nil {
			w.log.Println(err.Error(), "Failed to select from usage table")
		}
		switch {
		case id_usage > 0:
			err = w.UpdateQuery("UPDATE usage SET RXBytes=?, TXBytes=?, Uptime=? WHERE id=?",
				peer.RXBytes,
				peer.TXBytes,
				peer.Uptime.Seconds(),
				id_usage,
			)
			if err != nil {
				w.log.Println(err.Error(), "Failed to update usage table")
			}
		default:
			id_usage, err = w.InsertQuery("INSERT INTO usage (IPAddress, PublicKey, RXBytes, TXBytes, Uptime, instance_id) VALUES (?, ?, ?, ?, ?, ?)",
				net.IP(addr[:]).String(),
				hex.EncodeToString(peer.Key),
				peer.RXBytes,
				peer.TXBytes,
				peer.Uptime.Seconds(),
				id_intance,
			)
			if err != nil {
				w.log.Println(err.Error(), "Failed to insert into usage table")
			}
		}

		id_coordinates, err := w.GetCoordinatesValue(peer.Coords)
		if err != nil {
			w.log.Println(err.Error(), "Failed to select from Coordinates table")
		}
		switch {
		case id_coordinates < 0:
			id_coordinates, err = w.InsertCoordinates(peer.Coords)
		}
		if err != nil {
			w.log.Println(err.Error(), "Failed to insert into Coordinates table")
		}

		var additional_data_id int64
		switch {
		case id_coordinates < 0:
			additional_data_id, err = w.GetValue("SELECT id FROM additional_data WHERE Port=? AND Priority=? AND Remote=? AND Coords IS NULL AND usage_id=?",
				peer.Port,
				peer.Priority,
				peer.Remote,
				id_usage)
			if err != nil {
				w.log.Println(err.Error(), "Failed to select from additional_data table")
			}
		default:
			additional_data_id, err = w.GetValue("SELECT id FROM additional_data WHERE Port=? AND Priority=? AND Remote=? AND Coords=? AND usage_id=?",
				peer.Port,
				peer.Priority,
				peer.Remote,
				id_coordinates,
				id_usage)
			if err != nil {
				w.log.Println(err.Error(), "Failed to select from additional_data table")
			}
		}

		switch {
		case id_coordinates < 0 && additional_data_id <= 0:
			_, err = w.InsertQuery("INSERT INTO additional_data (Port, Priority, Remote, usage_id) VALUES (?, ?, ?, ?)",
				peer.Port,
				peer.Priority,
				peer.Remote,
				id_usage)
			if err != nil {
				w.log.Println(err.Error(), "Failed to insert into database")
			}
		case additional_data_id <= 0:
			_, err = w.InsertQuery("INSERT INTO additional_data (Port, Priority, Remote, Coords, usage_id) VALUES (?, ?, ?, ?, ?)",
				peer.Port,
				peer.Priority,
				peer.Remote,
				id_coordinates,
				id_usage)
			if err != nil {
				w.log.Println(err.Error(), "Failed to insert into database")
			}
		}
	}
}

func AddSessions(w *DBWriter, id_intance int64) {
	sessions := w.core.GetSessions()
	for _, session := range sessions {
		addr := address.AddrForKey(session.Key)

		id_session, err := w.GetValue("SELECT id FROM sessions WHERE IPAddress=? AND PublicKey=? AND instance_id=?",
			net.IP(addr[:]).String(),
			hex.EncodeToString(session.Key),
			id_intance)
		if err != nil {
			w.log.Println(err.Error(), "Failed to select from sessions table")
		}
		switch {
		case id_session > 0:
			err = w.UpdateQuery("UPDATE sessions SET RXBytes=?, TXBytes=?, Uptime=? WHERE id=?",
				session.RXBytes,
				session.TXBytes,
				session.Uptime.Seconds(),
				id_session)
			if err != nil {
				w.log.Println(err.Error(), "Failed to update database")
			}
		default:
			_, err := w.InsertQuery("INSERT INTO sessions (IPAddress, PublicKey, RXBytes, TXBytes, Uptime, instance_id) VALUES (?, ?, ?, ?, ?, ?)",
				net.IP(addr[:]).String(),
				hex.EncodeToString(session.Key),
				session.RXBytes,
				session.TXBytes,
				session.Uptime.Seconds(),
				id_intance)
			if err != nil {
				w.log.Println(err.Error(), "Failed to insert into database")
			}
		}
	}
}

func (w *DBWriter) AddData() {
	id, err := w.InsertQuery("INSERT INTO Instance_Sessions (Timestamp) VALUES (?)", time.Now())
	if err != nil {
		close(w.done)
	}
	for {
		select {
		case <-w.done:
			w.db.Close()
			return
		default:
			AddPeers(w, id)
			AddSessions(w, id)
		}
	}
}

func (w *DBWriter) InsertQuery(query string, args ...interface{}) (int64, error) {
	tx, err := w.db.Begin()
	if err != nil {
		return -1, err
	}
	res, err := tx.Exec(query, args...)
	if err != nil {
		tx.Rollback()
		return -1, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		tx.Rollback()
		return -1, err
	}
	tx.Commit()
	return id, nil
}

func (w *DBWriter) UpdateQuery(UpdateQuery string, args ...interface{}) error {
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}
	_, err = tx.Exec(UpdateQuery, args...)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
}

func (w *DBWriter) InsertCoordinates(coords []uint64) (int64, error) {
	var query string
	switch len(coords) {
	case 0:
		return -1, nil
	case 1:
		query = "INSERT INTO Coordinates (X) VALUES (?)"
	case 2:
		query = "INSERT INTO Coordinates (X, Y) VALUES (?, ?)"
	case 3:
		query = "INSERT INTO Coordinates (X, Y, Z) VALUES (?, ?, ?)"
	case 4:
		query = "INSERT INTO Coordinates (X, Y, Z, T) VALUES (?, ?, ?, ?)"
	case 5:
		query = "INSERT INTO Coordinates (X, Y, Z, T, N) VALUES (?, ?, ?, ?, ?)"
	default:
		return -1, errors.New("Too many coordinates")
	}
	coordinates := make([]interface{}, len(coords))
	for i, v := range coords {
		coordinates[i] = v
	}
	id, err := w.InsertQuery(query, coordinates...)
	if err != nil {
		return -1, err
	}
	return id, nil
}

func (w *DBWriter) GetCoordinatesValue(coords []uint64) (int64, error) {
	var query string
	switch len(coords) {
	case 0:
		return -1, nil
	case 1:
		query = "SELECT id FROM Coordinates WHERE X=?"
	case 2:
		query = "SELECT id FROM Coordinates WHERE X=? AND Y=?"
	case 3:
		query = "SELECT id FROM Coordinates WHERE X=? AND Y=? AND Z=?"
	case 4:
		query = "SELECT id FROM Coordinates WHERE X=? AND Y=? AND Z=? AND T=?"
	case 5:
		query = "SELECT id FROM Coordinates WHERE X=? AND Y=? AND Z=? AND T=? AND N=?"
	default:
		return -1, errors.New("Too many coordinates")
	}
	coordinates := make([]interface{}, len(coords))
	for i, v := range coords {
		coordinates[i] = v
	}
	id, err := w.GetValue(query, coordinates...)
	if err != nil {
		return -1, err
	}
	return id, nil
}

func (w *DBWriter) GetValue(query string, args ...interface{}) (int64, error) {
	var value int64
	err := w.db.QueryRow(query, args...).Scan(&value)
	if err != nil {
		return -1, err
	}
	return value, nil
}

func New(c *core.Core, log core.Logger) (*DBWriter, error) {
	db, err := initDB()
	if err != nil {
		return nil, err
	}
	s := DBWriter{
		core: c,
		db:   db,
		log:  log,
	}
	s.done = make(chan struct{})
	go s.AddData()
	return &s, nil
}

func (w *DBWriter) Stop() error {
	if w == nil {
		return nil
	}
	if w.db != nil {
		select {
		case <-w.done:
		default:
			close(w.done)
		}
		return w.db.Close()
	}
	return nil
}
