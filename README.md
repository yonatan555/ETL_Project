# ETL_Project

THIS GOAL PROJECT is to transform data from MySQL to MongoDB,
make manipulations and pass a configured file from mongodb.
this project will read the configured file and will behave according to it.

for example Mongodb configure
{
    "_id": "copyFirstNames",
    "source": {
        "class": "SqlReader",
        "connectionUrl": "jdbc:mysql://root:root@localhost:3306/students",
        "query": "SELECT * FROM students.firstnames;",
        "batchSize": {
            "$numberInt": "0"
        },
        "driverClass": "com.mysql.cj.jdbc.Driver"
    },
    "target": {
        "class": "UpsertMongoWriter",
        "collection": "allNames",
        "set": "first",
        "keys": ["id"]
    }
}
