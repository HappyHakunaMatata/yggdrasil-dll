package yggdrasildb

import (
	"database/sql"
	"testing"
)

type TestData struct {
	addr     string
	port     uint64
	key      string
	RXBytes  uint64
	TXBytes  uint64
	Uptime   float64
	priority uint8
	remote   string
	coords   []uint64
}

var data = []TestData{
	{
		addr:     "insert_test_address",
		port:     1,
		key:      "insert_test_key",
		RXBytes:  100,
		TXBytes:  100,
		Uptime:   100,
		priority: 0,
		remote:   "insert_test_link",
		coords:   []uint64{0, 0, 0},
	},
	{
		addr:     "insert_test_address",
		port:     1,
		key:      "insert_test_key",
		RXBytes:  1,
		TXBytes:  1,
		Uptime:   1,
		priority: 0,
		remote:   "insert_test_link",
		coords:   []uint64{0, 0, 0},
	},
	{
		addr:     "insert_test_address",
		port:     0,
		key:      "insert_test_key",
		RXBytes:  100,
		TXBytes:  100,
		Uptime:   100,
		priority: 1,
		remote:   "insert_test_link",
		coords:   []uint64{0, 1, 0},
	},
	{
		addr:     "new_insert_test_address",
		port:     0,
		key:      "new_insert_test_key",
		RXBytes:  100,
		TXBytes:  100,
		Uptime:   100,
		priority: 1,
		remote:   "new_insert_test_link",
		coords:   []uint64{0, 1, 0},
	},
}

func TestDbInit(t *testing.T) {
	if _, err := initDB(); err != nil {
		t.Fatal("Init error")
	}
}

func TestInsert(t *testing.T) {
	DropTable()
	db, err := initDB()
	if err != nil {
		t.Fatal("Error to init DB")
	}
	w := DBWriter{
		db: db,
	}
	if id, err := w.InsertQuery("INSERT INTO usage (IPAddress, PublicKey, RXBytes, TXBytes, Uptime, instance_id) VALUES (?, ?, ?, ?, ?, ?)",
		data[0].addr, data[0].key, data[0].RXBytes, data[0].TXBytes, data[0].Uptime, 0); err != nil || id != 1 {
		t.Fatal(err.Error())
	}
	if id, err := w.InsertQuery("INSERT INTO usage (IPAddress, PublicKey, RXBytes, TXBytes, Uptime, instance_id) VALUES (?, ?, ?, ?, ?, ?)",
		data[3].addr, data[3].key, data[3].RXBytes, data[3].TXBytes, data[3].Uptime, 0); err != nil || id != 2 {
		t.Fatal(err.Error())
	}
	if err := w.UpdateQuery("UPDATE usage SET RXBytes=?, TXBytes=?, Uptime=? WHERE IPAddress=? AND PublicKey=? AND instance_id=?",
		data[1].RXBytes, data[1].TXBytes, data[1].Uptime, data[1].addr, data[1].key, 0); err != nil {
		t.Fatal("Update error")
	}
	if id, err := w.GetValue("SELECT id FROM usage WHERE IPAddress=? AND PublicKey=? AND instance_id=?",
		data[1].addr, data[1].key, 0); err != nil || id != 1 {
		t.Fatal("Select error")
	}

}

/*
	func TestValue(t *testing.T) {
		db, err := initDB()
		if err != nil {
			t.Fatal("Error to init DB")
		}
		w := DBWriter{
			db: db,
		}
		count, err := w.Find(Queries.select_usage,
			data[1].addr,
			data[1].key)
		if err != nil {
			t.Fatal(err.Error(), "Record does not exist")
		}
		if count > 0 {
			if err := w.UpdateUsage(data[1].addr, data[1].port, data[1].key, data[1].RXBytes,
				data[1].TXBytes, data[1].Uptime, data[1].priority, data[1].remote, data[1].coords, 0); err != nil {
				t.Fatal("Fisrt update error")
			}
			if err := w.UpdateUsage(data[2].addr, data[2].port, data[2].key, data[2].RXBytes,
				data[2].TXBytes, data[2].Uptime, data[2].priority, data[2].remote, data[2].coords, 0); err != nil {
				t.Fatal("Second update error")
			}
			var key int
			var value int
			if err := w.db.QueryRow("SELECT id FROM usage WHERE IPAddress=? AND PublicKey=?", data[1].addr, data[1].key).Scan(&key); err != nil {
				t.Fatal("Key does not exist")
			}
			if err := w.db.QueryRow("SELECT COUNT(*) FROM additional_data WHERE usage_id=?", key).Scan(&value); err != nil {
				t.Fatal("Select error")
			}
			if value != 2 {
				t.Fatal("Count of records does not equal to 2")
			}
		}
	}

	func TestTrigger(t *testing.T) {
		db, err := initDB()
		if err != nil {
			t.Fatal("Error to init DB")
		}
		w := DBWriter{
			db: db,
		}
		if err := w.InsertUsage(data[0].addr, data[0].port, data[0].key, data[0].RXBytes,
			data[0].TXBytes, data[0].Uptime, data[0].priority, data[0].remote, data[0].coords, 0); err != nil {
			t.Fatal("Insert error")
		}
		if err := w.UpdateUsage(data[1].addr, data[1].port, data[1].key, data[1].RXBytes,
			data[1].TXBytes, data[1].Uptime, data[1].priority, data[1].remote, data[1].coords, 0); err != nil {
			t.Fatal("Update error")
		}
		var value int
		if value, err = w.Find(Queries.select_usage, data[0].addr, data[0].key); err != nil {
			t.Fatal("record does not found")
		}
		if value != 1 {
			t.Fatal("value is not equal to 1")
		}
		if err := w.db.QueryRow("SELECT RXBytes FROM usage WHERE IPAddress=? AND PublicKey=?", data[1].addr, data[1].key).Scan(&value); err != nil {
			t.Fatal(err.Error())
		}
		if value != 101 {
			t.Fatal("Rxbytes should be 101")
		}
	}*/

func TestFind(t *testing.T) {
	DropTable()
	db, err := initDB()
	if err != nil {
		t.Fatal(err)
	}
	w := DBWriter{
		db: db,
	}
	var id int64
	var value int64
	if id, err = w.InsertQuery("INSERT INTO additional_data (Port, Priority, Remote, Coords, usage_id) VALUES (?, ?, ?, ?, ?)",
		data[0].port, data[0].priority, data[0].remote, 0, 0); err != nil {
		t.Fatal(err)
	}
	if value, err = w.GetValue("SELECT id FROM additional_data WHERE Port=? AND Priority=? AND Remote=? AND Coords=? AND usage_id=?",
		data[0].port, data[0].priority, data[0].remote, 0, 0); err != nil {
		t.Fatal(err)
	}
	if id != value {
		t.Fatal(err)
	}

	if id, err = w.InsertQuery("INSERT INTO additional_data (Port, Priority, Remote, usage_id) VALUES (?, ?, ?, ?)",
		data[0].port, data[0].priority, data[0].remote, 1); err != nil {
		t.Fatal(err)
	}
	if value, err = w.GetValue("SELECT id FROM additional_data WHERE Port=? AND Priority=? AND Remote=? AND Coords IS NULL AND usage_id=?",
		data[0].port, data[0].priority, data[0].remote, 1); err != nil {
		t.Fatal(err)
	}
	if id != value {
		t.Fatal(err)
	}
}

/*
	func TestAddPeers(t *testing.T) {
		DropTable()
		db, err := initDB()
		if err != nil {
			t.Fatal("Error to init DB")
		}
		w := DBWriter{
			db: db,
		}

		if err := w.InsertUsage(data[0].addr, data[0].port, data[0].key, data[0].RXBytes,
			data[0].TXBytes, data[0].Uptime, data[0].priority, data[0].remote, data[0].coords, 0); err != nil {
			t.Fatal("Insert error")
		}
		count, err := w.Find(Queries.select_usage,
			data[1].addr,
			data[1].key)
		if count > 0 {
			if err := w.UpdateUsage(data[1].addr, data[1].port, data[1].key, data[1].RXBytes,
				data[1].TXBytes, data[1].Uptime, data[1].priority, data[1].remote, data[1].coords, 0); err != nil {
				t.Fatal("Update error")
			}
		} else {
			if err := w.InsertUsage(data[1].addr, data[1].port, data[1].key, data[1].RXBytes,
				data[1].TXBytes, data[1].Uptime, data[1].priority, data[1].remote, data[1].coords, 0); err != nil {
				t.Fatal("Insert error")
			}
		}
		if count, _ := w.Find(Queries.select_usage,
			data[1].addr,
			data[1].key); count != 1 {
			t.Fatal("Count not equal to 1")
		}
	}*/

func DropTable() {
	database, _ := sql.Open("sqlite3", "/Users/main/Projects/yggdrasil-go/src/yggdrasildb/usage.db")
	database.Exec("DROP TABLE IF EXISTS usage")
	database.Exec("DROP TABLE IF EXISTS additional_data")
	database.Exec("DROP TABLE IF EXISTS Sessions")
	database.Exec("DROP TABLE IF EXISTS Coordinates")
	database.Close()
}

func TestCoords(t *testing.T) {
	DropTable()
	db, err := initDB()
	if err != nil {
		t.Fatal(err)
	}
	w := DBWriter{
		db: db,
	}
	if _, err := w.InsertCoordinates([]uint64{1, 0, 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.InsertCoordinates([]uint64{1}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.InsertCoordinates([]uint64{}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.InsertCoordinates([]uint64{0, 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.InsertCoordinates([]uint64{0, 2, 0, 4}); err == nil {
		t.Fatal(err)
	}
	if id, err := w.GetCoordinatesValue([]uint64{0, 2}); err != nil || id != 3 {
		t.Fatal(err)
	}
	if id, err := w.GetCoordinatesValue([]uint64{1, 0, 3}); err != nil || id != 1 {
		t.Fatal(err)
	}
	if id, err := w.GetCoordinatesValue([]uint64{1, 0, 2}); err == nil || id > 0 {
		t.Fatal(err)
	}
}
