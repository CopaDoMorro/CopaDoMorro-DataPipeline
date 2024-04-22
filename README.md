# CopaDoMorro-DataPipeline

Repositório de Armazenamento das DAGs do Apache Airflow utilizadas para os procedimentos de ETL da Copa do Morro.

## Como utilizar?

1 - Atualizar as dependências do Ubuntu:

```
sudo apt update
sudo apt upgrade
```

2 - Adicionar o repositório da versão específica do Python:
```
sudo add-apt-repository ppa:deadsnakes/ppa
```

3 - Instalar Python3.9:
```
sudo apt install python3.9
python3.9 --version
```

4 - Instalar pacote de ambientes virtuais do Python3.9:
```
sudo apt install python3.9-venv
```

5 - Crie um ambiente virtual na máquina através do Python3.9:
```
python3.9 -m venv venv
```

6 - Ativar o ambiente virtual do Python3.9:
```
source venv/bin/activate
```

7 - Instalar os requisitos:
```
pip install -r requirements.txt"
```

8 - Exportar variável de ambiente da pasta de trabalho do Airflow:
```
export AIRFLOW_HOME=~/Documents/CopaDoMorro-DataPipeline
```

9 - Executar o Airflow no modo Standalone e executar as pipelines:
```
airflow standalone
```

## Conclusão

Após a configuração, no terminal ficará as credenciais de acesso do Airflow WebServer localizado em **localhost:8080**. 
A interace Web serve somente para acompanhar a execução, não existe maneira "clicável" de criar ou editar pipelines...
