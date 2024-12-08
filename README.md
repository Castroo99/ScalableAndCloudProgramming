# ScalableAndCloudProgramming
Repository for the project Scalable and Cloud Programming

## Run Scala Code
Go to the folder where is placed the script you want to execute and run the commands:

```
sbt compile
sbt run
``` 
## Setup Env Table
Inserire nelle variabili d'ambiente la coppia (chiave, valore)
```
SBT_OPTS, -Xmx4G -Xms2G
```

## Setup Python Virtual Environment

Run the command:
```
python -m venv .venv
``` 

Activate the virtual environment in Windows:

``` 
.venv\Scripts\activate
``` 

Activate the virtual environment in Mac OS/Linux:

``` 
source .venv/bin/activate
``` 

Now you can install all the libraries you want in your venv