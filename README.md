
![9ad3c058-d029-47c4-a2df-68cbcbf41e0a](https://github.com/user-attachments/assets/4b77a097-ea68-4e95-85f0-639785b93c7b)
# AdiDB - Building a Database from Scratch

## Introduction

Hey there! 👋 This is my journey of building a database from scratch using Go. I really wanted to understand how databases actually work under the hood, but quickly turned into a fascinating deep-dive into database internals.

## Features I've Implemented

### ACID Transactions
After many late nights and countless cups of coffee, I've implemented full ACID compliance:
- **Atomicity**: All or nothing - transactions either fully complete or fully roll back
- **Consistency**: The database remains valid after every transaction
- **Isolation**: Concurrent transactions don't step on each other's toes
- **Durability**: Once committed, data stays committed (yes, even if your machine crashes!)

### Concurrent Transaction Handling
This was probably the trickiest part! The engine supports:
- Multiple transactions running simultaneously
- Deadlock detection
- Lock management to prevent dirty reads/writes
- Transaction isolation levels

### B+ Trees for Indexing
I chose B+ Trees because they're practically the industry standard for database indexes. My implementation includes:
- Efficient range queries
- Auto-balancing on insert/delete
- Disk-friendly node structure
- Configurable node size

### SQL-like Query Language
While it's not full SQL (yet!), the query language supports basic operations:
```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    age INT
)

INSERT INTO users (name, age) VALUES ("Alice", 30)

UPDATE users SET age = 31 WHERE id = 1

SELECT name, age FROM users
```

## Project Status

This is very much a learning project and a work in progress. While it works, there's still a lot I want to add:
- [x] Basic CRUD operations
- [x] ACID transactions
- [x] B+ Tree indexes
- [x] Table creation and schema management
- [x] Simple query parser
- [ ] Implement Raft Algorithm
- [ ] Make it a proper distributed database

## Why Build This?

I believe the best way to truly understand how something works is to build it from scratch. While I wouldn't recommend using this in production, building this has taught me more about databases than using them.

## Learning Resources

If you're interested in building your own database, here are some resources I found incredibly helpful:
- [Database Internals](https://www.databass.dev) book
- [Build Your Own](https://build-your-own.org/) book

## Contributing

Feel free to open issues or PRs if you spot bugs or have suggestions! While this is primarily a learning project, I'm always happy to collaborate and learn from others.

## License

MIT

---

*Built with Go and lots of debugging sessions* 🔍
