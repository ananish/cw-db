# Java SQL Database Server (BNF Grammar Based)

A database server developed for my **"Object-Oriented Programming with Java"** module coursework. This project implements a file-based relational database that communicates using a simplified SQL-like language, fully defined by a custom BNF grammar (`grammar.txt`).

---

## 🧠 Language Support

The server processes queries written in a **condensed version of SQL**, defined by a strict **BNF grammar**. Unlike traditional interpreters using an AST, this implementation:
- **Validates input using regex** based on BNF rules
- **Tokenizes** the input
- **Executes** commands directly without tree-based interpretation

All queries must end with a `;` and follow the formatting described in `grammar.txt`.

---

## 🧪 Error Handling

- Queries are **strictly validated** using the BNF grammar before execution.
- If the query does not match the expected grammar, it is **rejected immediately** with a clear error message.
- Ensures only syntactically correct and meaningful SQL-like commands are processed.

---

## 🖥️ Database Server

The server is a **TCP socket-based application** that:
- Accepts queries from connected clients (via `DBClient`)
- Parses and validates them
- Executes supported commands
- Sends results or error messages back over the socket

---

## 💾 Data Storage

- Data is stored persistently in the local file system using **`.tab` files**
- Each database is a folder inside `databases/`
- Each table is a tab-separated file, with:
  - First line = column headers (including `id`)
  - Subsequent lines = row data
- The server **reads these files into memory** before query execution and **writes them back** afterward

---

## ✅ Commands Supported

These commands are fully implemented:

| SQL Command | Description |
|-------------|-------------|
| `CREATE DATABASE dbname;` | Creates a new database folder |
| `USE dbname;`             | Switches to the selected database |
| `CREATE TABLE tname (col1, col2);` | Creates a table with auto `id` |
| `INSERT INTO tname VALUES ('v1', 'v2');` | Adds a new row |
| `SELECT * FROM tname;`    | Retrieves all rows and columns |
| `SELECT col1, col2 FROM tname;` | Retrieves selected columns |

---

## 🧰 Technologies Used

- Java (Core, File I/O, Collections)
- TCP Sockets (`ServerSocket`, `Socket`)
- Custom SQL interpreter with **BNF grammar validation**
- Recursive value extraction and CSV-style `.tab` file management

---

## 🔧 How to Use

### Compile
```bash
javac -d out src/edu/uob/*.java
