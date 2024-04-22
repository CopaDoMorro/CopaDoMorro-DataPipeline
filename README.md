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
pip install -r requirements.txt
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

## Query de Consulta

Ao concluir a execução do Pipeline, será possível acessar os dados no Data Warehouse através da query:

```
SELECT DISTINCT
	dw.jogador.nome as nome_jogador,
	dw.jogador.cpf as cpf_jogador,
	dw.jogador.data_nascimento as data_nascimento_jogador,
	dw.jogador.estado as estado_jogador,
	dw.jogador.cidade as cidade_jogador,
	dw.jogador.comunidade as comunidade_jogador,
	dw.jogo.nome as nome_jogo,
	dw.jogo."data"  as data_jogo,
	dw.jogo.estado  as estado_jogo,
	dw.jogo.cidade  as cidade_jogo,
	dw.jogo.comunidade as comunidade_jogo,
	dw.responsavel.nome as nome_reponsavel,
	dw.responsavel.cpf as cpf_responsavel,
	dw.responsavel.email as email_responsavel
FROM
	dw.jogo_jogador
JOIN
	dw.jogador ON dw.jogador.id = dw.jogo_jogador.id_jogador
JOIN
	dw.jogo ON dw.jogo.id = dw.jogo_jogador.id_jogo
JOIN
	dw.responsavel ON dw.responsavel.id = dw.jogo_jogador .id_responsavel
```
