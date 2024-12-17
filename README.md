# ScalableAndCloudProgramming
Repository for the project Scalable and Cloud Programming

## Run Scala Code
```
sbt compile
sbt run
``` 
## Setup Env Table
Inserire nelle variabili d'ambiente la coppia (chiave, valore)
```
SBT_OPTS, -Xmx4G -Xms2G
```

## Esecuzione su Google Cloud Platform
Per eseguire il progetto su Google Cloud Platform è necessario creare l'eseguibile jar del progetto con tutte le sue dipendenze. Per fare ciò è necessario eseguire, all'interno della cartella del modello scelto, il comando:
```
sbt assembly
```
Questo comando creerà un file jar all'interno della cartella `target/scala-2.12/` che potrà essere utilizzato per eseguire il progetto su Google Cloud Platform.

Dopo aver creato il file jar, è necessario configurare l'ambiente di lavoro su Google Cloud Platform. Per fare ciò è necessario:
- Creare un progetto su Google Cloud Platform
- Creare un bucket su Google Cloud Storage
- Abilitare la Google Cloud Dataproc API
- Creare un cluster Dataproc 

Una volta configurato l'ambiente di lavoro, è necessario caricare il file jar e il dataset sul bucket creato in precedenza.

Per esequire il jar su GCP è necessario creare un Job di lavoro su Dataproc specificando il path del file jar (nel bucket come `URI gsutil`) e il cluster (creato in precedenza).





