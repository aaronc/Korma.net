# Korma.net

Tasty SQL for ClojureCLR.

## Getting started

This is a port of Korma to ClojureCLR.  Only MySql is supported right now.
In order for MySql to work, MySql.Data.dll (from MySQL Connector/NET) will need
to be loaded into the current AppDomain (the assembly-load functions can be
used for this).  The map of options passed to the `mysql` function is used to
generate the MySql connection string (see the connection string options
[here](http://dev.mysql.com/doc/refman/5.6/en/connector-net-connection-options.html),
as well as the example below).

For docs and real usage, check out http://sqlkorma.com


## Examples of generated queries:

```clojure

(use 'korma.db)
(defdb db (mysql {:Host "localhost"
                  :Database "mydb"
                  :User "user"
		  :Port "3306"
		  :Password "dbpass"}))

(use 'korma.core)
(defentity users)

(select users)
;; executes: SELECT * FROM users

(select users
  (fields :usersname :id))
;; executes: SELECT users.usersname, users.id FROM users

(select users
  (where {:usersname "chris"}))
;; executes: SELECT * FROM users WHERE (users.usersname = 'chris)'

(select users 
  (where {:active true})
  (order :created)
  (limit 5)
  (offset 3))
;; executes: SELECT * FROM users WHERE (users.active = TRUE) ORDER BY users.created DESC LIMIT 5 OFFSET 3

(select users
  (where (or (= :usersname "chris")
             (= :email "chris@chris.com"))))
;; executes: SELECT * FROM users WHERE (users.usersname = 'chris' OR users.email = 'chris@chris.com')

(select users
  (where {:usersname [like "chris"]
          :status "active"
          :location [not= nil]))
;; executes SELECT * FROM users WHERE (users.usersname LIKE 'chris' AND users.status = 'active' AND users.location IS NOT NULL)

(select users
  (where (or {:usersname "chris"
              :first "chris"}
             {:email [like "%@chris.com"]})))
;; executes: SELECT * FROM users WHERE ((users.usersname = 'chris' AND users.first = 'chris') OR users.email LIKE '%@chris.com)'


(defentity address
 (table-fields :street :city :zip))

(defentity users
 (has-one address))

(select users
 (with address))
;; SELECT address.street, address.city, address.zip FROM users LEFT JOIN address ON users.id = address.users_id

```

## License

Copyright (C) 2011 Chris Granger

Distributed under the Eclipse Public License, the same as Clojure.
