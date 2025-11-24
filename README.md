# whatsapp-automation-mk-II
whatsapp automation using Selenium, MongoDB and mcp

# improvments:
 selenium driver now have session persistence and retrys if not read messages and runs fasster 
 can read messages in 2 sender configuration: name and phone  
 
 structure:
project/
│
├─ src/
│   ├─ etl/
|   ├─  ├─ __init__.py    
│   │   ├─ extract.py
│   │   ├─ transform.py
│   │   └─ load.py
│   ├─ secrets/
│   │   └─ key.json
│   ├─ sheets_connect.py
│   └─ __init__.py
├─ __init__.py
│
└─ .gitignore


#end goal: create an ETL to read the data from whatsapp write it to a db and update the sheets and make all of this dockrized

#containers: db, app, future(ui --> mcp), connect sales team message read on the same read from the students group