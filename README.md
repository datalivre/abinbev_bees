# BEES Data Engineering - Breweries Case

## Visão Geral

A Open Brewery DB é uma API gratuita e de código aberto que disponibiliza dados públicos sobre cervejarias e cidrarias. Mantida pela comunidade, ela serve como uma fonte valiosa para desenvolvedores e analistas que buscam integrar esses dados em projetos de análise ou aplicações.

Este documento descreve o processo de consumo, transformação e persistência dos dados, seguindo a arquitetura Medallion (Bronze → Silver → Gold), com foco em eficiência, custo e escalabilidade.

### Ferramentas e Ambiente

A solução foi implementada na nuvem da Microsoft (Azure) Free Trial, com os seguintes componentes principais
- Databricks: Coding
- Databricks Workflows: Orquestração do pipeline.
- Delta Lake: Armazenamento com garantias ACID e integração ao Unity Catalog.
- Autoloader: Ingestão eficiente de novos dados.
- Github: hospedagem de código-fonte e controle de versão.

### Decisões de Design e Trade-offs
#### 1. Orquestração: Workflows vs. Azure Data Factory

**Proposta inicial**:
- Usar Azure Data Factory para consumo da API e ingestão na camada de pré-landing (acionado diariamente).
- Databricks Workflows para processar as camadas bronze, silver e gold (acionado por file arrival).

**Problema**:
- Duplicidade de ferramentas aumentaria custos operacionais e complexidade de integração.

**Solução adotada**:
- Databricks Workflows como orquestrador único:
  - Elimina a necessidade de conexões entre serviços.
  - Oferece suporte nativo a Delta Lake e Autoloader.
  - Reduz a curva de aprendizado, custos e simplifica a manutenção.

#### 2. Camada de Armazenamento: Delta Lake vs. Data Lake
- Delta Lake foi escolhido por ser a evolução natural do Data Lake, trazendo:
  - Transações ACID: Garantia de consistência em atualizações.
  - Unity Catalog: Controle de acesso e governança unificada.
  - Otimizações nativas: Z-ordering, compactação e time travel. 

### Arquitetura do Pipeline
O fluxo foi dividido em três camadas:
- Bronze (Raw):
  - Dados brutos da API, preservando a estrutura original.
- Silver (Cleaned):
  - Dados ingestionados via Autoloader com checkpointing.
  - Dados limpos, tipados e enriquecidos.
  - Dados particionados por atributos estratégicos (country).

- Gold (Analytics):
  - Agregações de acordo com o negócio.
  - Otimizadas com Z-ordering para consultas rápidas.

### Por Que Esta Solução?
Esta abordagem garante um pipeline escalável, econômico e pronto para produção, alinhado às melhores práticas de engenharia de dados modernas.
- Custo-efetividade: Elimina serviços redundantes.
- Simplicidade: Integração nativa entre Databricks e Delta Lake.
- Governança: Unity Catalog assegura segurança e rastreabilidade.

## Configuração Inicial do Ambiente
O projeto teve início com a criação de uma instância de Databricks diretamente no portal Microsoft Azure.

![image](https://github.com/user-attachments/assets/9ac671fb-57f4-494c-8de8-46cc6c575a4e)

### Hierarquia de Dados no Unity Catalog
O Databricks utiliza uma estrutura de namespace de três níveis no Unity Catalog: `catálogo.Esquema.Tabela`, sendo que:
- Catálogo: Nível mais alto de organização - equivalente a um "data warehouse"
- Esquema: Contêiner lógico para tabelas e views - equivalente a bancos de dados tradicionais
- Tabelas/Views: Objetos de dados com esquema definido

### Estrutura do Unity Catalog
Para organizar os dados conforme a arquitetura medalhão, foram definidos três esquemas dedicados dentro do catálogo principal:
1. db_bronze_dev
- **Comando**: `create database if not exists ws_bees_dev.db_bronze_dev;`
- **Finalidade**: Persistência de dados brutos (ingestão inicial)
- **Características**:
  - Persistir dados brutos
  - Não adiciona limpeza ou transformação nos dados
  
2. db_silver_dev
- **Comando**: `create database if not exists ws_bees_dev.db_silver_dev;`
- **Finalidade**: Camada de limpeza e transformação
- **Processos aplicados**:
  - Tipagem de campos
  - Particionamento

3. db_gold_dev
- **Comando**: `create database if not exists ws_bees_dev.db_gold_dev;`
- **Finalidade**: Camada analítica com regras de negócio
- **Saída**:
  - Regras ligadas ao negócio
  - Dados prontos para consumo em BI e analytics
 
### Estrutura de Armazenamento do Azure Data Lake
De forma análoga, foram criados três contêineres dedicados na conta de armazenamento do Azure:
#### 1. ctr-bz-dev
- **Função**: Armazenar dados brutos (camada Bronze)
- **External Locations**: `abfss://ctr-bz-dev@stgbeesdev.dfs.core.windows.net`
   
#### 2. ctr-sl-dev
- **Função**: Armazenar dados transformados (camada Silver)
- **External Locations**: `abfss://ctr-sl-dev@stgbeesdev.dfs.core.windows.net` 
     
#### 3. ctr-gd-dev
- **Função**: Armazenar dados analíticos para consumo de outras equipes (camada Gold)
- **External Locations**: `abfss://ctr-gd-dev@stgbeesdev.dfs.core.windows.net`

### Versionamento com GitHub
![image](https://github.com/user-attachments/assets/dccb584c-0d38-4a1c-9ab6-afff43c130c0)

- **Autenticação**: Token de acesso pessoal com escopo Repos
- **Fluxo**:
  - Geração do token na interface do GitHub
  - Configuração no Databricks Repos
  - Sincronização automática de código

- **Vantagens**:
    ✔ Rastreabilidade de mudanças
    ✔ CI/CD integrado
    ✔ Backup do código-fonte
  
## Estrutura de Pastas e Convenções de Nomenclatura
### Visão Geral da Organização
A estrutura de pastas foi projetada para garantir clareza, modularidade e escalabilidade no gerenciamento de projetos de engenharia de dados, seguindo um padrão intuitivo baseado em camadas e origens de dados.

### Hierarquia de Pastas
```
abinbev_bees/
└── DATA_ANALYTICS/
└── DATA_ENGINEERING/
    └── OPENBREWERY/
        └── API/
            ├── SRC_BZ_OPENBREWERY_API_INCREMENT
            ├── BZ_SL_OPENBREWERY_API_INCREMENT
            └── SL_GD_OPENBREWERY_API_INCREMENT
```
### Princípios de Organização
#### 1. Separação por Domínio
- DATA_ENGINEERING: Projetos dedicados a pipelines de dados.
- DATA_ANALYTICS: Pastas irmãs para outros fluxos (evitando mistura de escopos).
#### 2. Projeto e Origem dos Dados
- Nível 1: OPENBREWERY (nome do projeto).
- Nível 2: API (fonte dos dados). Se houver uma nova fonte (ex.: SHAREPOINT), uma pasta paralela será criada.

#### 3. Notebooks por Camada
- Cada notebook representa um estágio do pipeline Medallion (Bronze → Silver → Gold).

### Convenção de Nomenclatura
Os nomes dos notebooks seguem um padrão claro que indica origem e destino dos dados:
- SRC_BZ: Os dados são coletados da origem e seu destino é a camada Bronze.
- BZ_SL: Os dados são coletados na camada Bronze e, depois de transformados, tem destino na camada Silver.
- SL_GD: Os dados são coletados na camada Silver e disponibilizados na camada Gold.
- Vantagens
    ✔ Autoexplicativo: A origem e o destino são identificáveis imediatamente.
    ✔ Consistência: Padrão aplicável a novos projetos.
    ✔ Facilidade de Manutenção: Rápida localização de notebooks por camada.


## Implementação da Camada Bronze
**Notebook**: `SRC_BZ_OPENBREWERY_API_INCREMENT`
**Contexto**:
- Primeira Tentativa: Uso do módulo openbrewery (sugerido pela API), que apresentou falhas críticas.
- Solução Definitiva: Reconstrução do pipeline com o módulo requests do Python, garantindo confiabilidade e controle refinado.

### Objetivos do Pipeline

- Extrair dados brutos da API Open Brewery DB
- Garantir qualidade com tratamento de erros e validações
- Otimizar desempenho com processamento paralelo
- Persistir em Delta Lake (camada Bronze)

### Solução Implementada
#### 1. Extração de Dados
- **Consulta à API**: A função `tt_pages` obtém metadados da API para determinar o número total de páginas de dados disponíveis. A função `fetch_data` retorna uma lista paginada de cervejarias.
```python
def tt_pages():
    try:
        return ceil(requests.get(metadados_url).json().get('total')/per_page)
    except Exception as e:
        raise(e)

def fetch_data(page):
    response = requests.get(domain+str(page))
    if response.status_code == 200:
        return response.json()
    else:
        print(f'Failed to get page data {page}: {response.status_code}')
        return None

```

- **Processamento paralelo**: Implementado um ThreadPoolExecutor para buscar as múltiplas páginas fornecidas por `tt_pages`, simultaneamente, reduzindo significativamente o tempo de extração.
```python
with ThreadPoolExecutor(max_workers=10) as executor:
    results = executor.map(fetch_data, range(1, tt_pages() + 1))
```
#### 2. Transformação de Dados
- **Estruturação de dados**: Converte a lista de resultados em um DataFrame Spark para processamento distribuído.
```python
for result in results:
    if result is not None:
        all_data.extend(result)
else:
    openbrewery_new_bronze_df = spark.createDataFrame(all_data).distinct()
```
- **Detecção de novos registros**: Compara os novos registros com os dados existentes na tabela Bronze usando left_anti para garantir que os registros sejam processados apenas uma vez.
```python
try:
    openbrewery_bronze_df = spark.read.table(bronze_table)
    if openbrewery_bronze_df.count() > 0:
        openbrewery_new_bronze_df = openbrewery_new_bronze_df.join(
            openbrewery_bronze_df, on='id', how='left_anti')
except Exception as err:
    print(err)
```
#### 3. Carga de Dados
- Controle de esquema: Utiliza mergeSchema=true para lidar com possíveis evoluções na estrutura da API.
- Persistência incremental: Grava apenas novos registros na tabela Bronze usando append.
- Otimização de tabela: Executa comando OPTIMIZE após carga para melhorar performance de consultas futuras.
```python
try:
    ...
except Exception as err:
    ...
finally:
    if openbrewery_new_bronze_df.count() > 0:
        openbrewery_new_bronze_df.write.option('mergeSchema', 'true').mode(
            'append').saveAsTable(bronze_table)
        spark.sql('OPTIMIZE '+bronze_table)
    else:
        print('No new records to load.')
```
### Desafios e Soluções
- Desafio: Latência na extração
  - Solução: ThreadPoolExecutor com 10 workers
- Desafio: Dados duplicados
  - Solução: `.distinct()` e `left_anti join`
- Desafio: Evolução do schema da API
  - Solução: `mergeSchema=True` para flexibilidade para mudanças
 

## Implementação da Camada Silver
**Notebook**: `BZ_SL_OPENBREWERY_API_INCREMENT`

### Visão Geral
Pipeline de streaming que transforma dados brutos (Bronze) em dados limpos e particionados (Silver), utilizando Delta Lake e Autoloader para processamento incremental.

### Objetivos do Pipeline
-  Transformar dados brutos em dados prontos para análise
-  Garantir qualidade com tipagem segura
-  Otimizar performance com particionamento e Z-Ordering
-  Processar incrementalmente novos dados (evitando reprocessamentos)

### Solução Implementada
#### 1. Configurações de Otimização
- **Otimização de escrita**: configuração de gerenciamento do tamanho dos arquivos para operações de escrita.
```py
spark.conf.set('spark.databricks.delta.optimizeWrite.enabled', 'true')
```
#### 2. Transformação de Dados (Bronze → Silver)
- **Tipagem segura**: Conversão explícita de campos.
```py
openbrewery_df = (
    spark.readStream.format('delta')
    .table(bronze_table)
    .withColumn('longitude', col('longitude').cast(DoubleType()))
    .withColumn('latitude', col('latitude').cast(DoubleType())))
```
- **Particionamento Estratégico**: Dados particionados por `country` para consultas mais rápidas.
- **Controle de Esquema**: `mergeSchema=True` permite evolução do schema evitando a falha do pipeline.
#### 3. Carga Incremental com Streaming
- **Trigger availableNow**: Processamento sob demanda (equivalente a um batch com eficiência de streaming).
```py
query = (
    openbrewery_df
    .writeStream.format('delta')
    .trigger(availableNow=True)
    .option('mergeSchema', 'true') 
    .option('checkpointLocation', bronze_checkpoint)
    .option('path', silver_path)
    .partitionBy('country')
    .toTable(silver_table))

query.awaitTermination()
```
- **Otimização Pós-Carga**: OPTIMIZE Z-Ordering para consultas filtradas por city e state.

### Desafios e Soluções
- Desafio: Dados geográficos como string
  - Solução: Conversão para DoubleType garantindo precisão
- Desafio: Consultas lentas por país
  - Solução: Particionamento por `country`
- Desafio: Evolução do schema da API
  - Solução: `mergeSchema=True` resiliente a mudanças
 
## Implementação da Camada Gold
**Notebook**: SL_GD_OPENBREWERY_API_INCREMENT
### Visão Geral
Pipeline de streaming que consolida dados da camada Silver em métricas analíticas prontas para consumo, utilizando agregações estratégicas e otimizações para desempenho.
### Objetivos do Pipeline
- Agregar dados por localização e tipo de cervejaria
- Garantir atualização constante com reprocessamento completo (outputMode="complete")
- Otimizar consultas com compactação e técnicas de armazenamento
### Solução Implementada
#### 1. Configurações e Otimização
- **Ajuste dinâmico de execução**: Melhora desempenho em agregações.
```py
spark.conf.set('spark.sql.adaptive.enabled', 'true')
```
- **Compactação automática**: Reduz tamanho dos arquivos gerados.
```py
spark.conf.set('spark.databricks.delta.optimizeWrite.enabled', 'true')
```
#### 2. Agregação Estratégica:
- Agrupa cervejarias por localização (state + country) e tipo (brewery_type).
- Calcula a quantidade por grupo.
```py
openbrewery_df = (
    spark.readStream
    .format('delta')
    .table(silver_table)
    .groupBy(
        concat_ws(', ', 'state', 'country').alias('location'),
        col('brewery_type').alias('type'))
    .agg(count('*').alias('quantity'))
```
#### 3. Carga Incremental com Streaming
- **Modo complete**:
  - Reescreve toda a tabela Gold a cada execução, garantindo dados atualizados.
  - Ideal para agregações que dependem de todos os dados históricos.
- **Trigger availableNow**:
  - Processamento sob demanda.
- **Checkpointing**:
  - Armazena metadados para recuperação em caso de falha.
- **Otimização Pós-Carga**:
  - Comando OPTIMIZE para acelerar consultas futuras.
    
```py
query = (
    openbrewery_df
    .writeStream
    .format('delta')
    .outputMode("complete")
    .trigger(availableNow=True)
    .option("checkpointLocation", silver_checkpoint)
    .option('path', gold_path)
    .toTable(gold_table))

query.awaitTermination()
_ = spark.sql('OPTIMIZE ' + gold_table)
```

### Desafios e Soluções
- Desafio: Performance em consultas
  - Solução: OPTIMIZE pós-carga
- Desafio: Recuperação de falhas
  - Solução: Checkpoint em storage externo
 
## Orquestração com Databricks Workflows
**Job**: `wf_openbrewery_api`
### Visão Geral
Workflow agendado no Databricks para executar o pipeline Bronze → Silver → Gold de forma automatizada e confiável, com tratamento de falhas e notificações.
Configuração do Workflow

![image](https://github.com/user-attachments/assets/30c58541-da7b-4dd6-82ab-89a7489b41a8)

#### 1. Agendamento (Schedule)
- Trigger diário (1): Execução automática uma vez por dia.
- Horário personalizável: Pode ser ajustado para horários de baixo uso de recursos.

#### 2. Tarefas (Tasks)
- Nomenclatura clara: Cada tarefa tem o mesmo nome do notebook correspondente (2):
  - SRC_BZ_OPENBREWERY_API_INCREMENT (Bronze)
  - BZ_SL_OPENBREWERY_API_INCREMENT (Silver)
  - SL_GD_OPENBREWERY_API_INCREMENT (Gold)

- Dependências em cascata (3):
  - Gold só executa após sucesso do Silver.
  - Silver só executa após sucesso do Bronze.
    
#### 3. Execução Sequencial
- Sem concorrência entre tarefas, garantindo ordem correta e consistência dos dados.

**Gerenciamento de Falhas**
#### 4. Notificações Automáticas (4)
- Canais suportados: E-mail, Microsoft Teams, Slack, etc.

#### 5. Política de Tentativas (5)
- Reexecução automática: Em caso de falha (ex.: timeout na API).
- Número de tentativas: Configurável (padrão: 1 ou mais).

**Vantagens da Abordagem**
✔ Visibilidade: Notificações mantêm a equipe informada sobre o status.
✔ Resiliência: Tentativas automáticas reduzem intervenção manual.

## Monitoramento de Workflows no Databricks
### Visão Geral
O Databricks Workflows oferece uma interface intuitiva para monitorar jobs em tempo real, com múltiplas visualizações e logs detalhados.

![image](https://github.com/user-attachments/assets/b03b0ea1-fb6f-416b-8a7e-dc3d82249394)

### Principais Recursos de Monitoramento
#### 1. Visualização de Execução
- Linha do Tempo (Timeline View):
  - Mostra a duração e o status de cada tarefa em um formato gráfico.
  - Identifica gargalos (ex.: tarefas com tempo de execução anormal).
  
  ![image](https://github.com/user-attachments/assets/dadaeb97-81ea-49be-9b69-4510289c14e4)

- Modo Lista:
  - Exibe todas as tarefas em ordem cronológica.
  - Inclui status (Sucesso, Falha, Em andamento).
  
  ![image](https://github.com/user-attachments/assets/52fd5f9a-9f47-4131-bae6-99a6601b338f)
#### 2. Detalhes da Execução
- Código Fonte: Dá acesso ao script exatamente como foi executado.
  
![image](https://github.com/user-attachments/assets/38782bd4-04ba-4b19-8b4d-9def9ff88a5c)

- Logs de Eventos (View run events):
  - logs que permitem acompanhar todos os eventos do job.
  
  ![image](https://github.com/user-attachments/assets/152d5d5c-0d86-4394-9ffb-bced4fa80b1f)

#### 3. Métricas de Performance
- Hardware: Uso de CPU, memória e disco.
- Spark: Tempo de execução por estágio, número de tarefas.
- GPU (se aplicável): Utilização e temperatura.

![image](https://github.com/user-attachments/assets/24ef9efc-e7fc-4f2d-98ef-599e9c2b0272)

#### 4. Spark UI Integrada
- Acesso direto à interface do Spark UI para análise de log detalhada:

![image](https://github.com/user-attachments/assets/c241867f-5277-417e-97b9-5ac8d15bfce2)

#### Usando para Troubleshooting
- Identificar Falhas:
  - Verifique o status na linha do tempo ou lista.
  - Clique em "View run events" para logs detalhados.

- Analisar Performance:
  - Acesse a Spark UI para investigar estágios lentos.
  - Verifique métricas de hardware para gargalos de recursos.

- Comparar Execuções:
  - Use o histórico de runs para detectar regressões.

**Vantagens da Abordagem**
✔ Transparência: Toda execução é rastreável.
✔ Debugging rápido: Logs centralizados e acessíveis.

## Governança de Dados com Unity Catalog
### Visão Geral
O Unity Catalog é a solução unificada do Databricks para governança, segurança e rastreabilidade de dados, oferece controle de acesso, linhagem de dados e histórico de operações.

### Principais Funcionalidades
#### 1. Controle de Acesso
- Baseado em Privilégios:
  - Permissões atribuídas a usuários ou grupos (SELECT, MODIFY, OWNER).
 
  ![image](https://github.com/user-attachments/assets/a77ff830-964c-46c7-888c-4e3fb88c5143)

- Exemplo de Comando:
```sql
GRANT SELECT ON TABLE db_gold_dev.tb_openbrewery_api TO `analytics-team`;
```
#### 2. Linhagem de Dados (Data Lineage)
- Rastreamento:
  - Visualiza o fluxo de dados desde a origem (SRC) até o destino (GD).

  ![image](https://github.com/user-attachments/assets/0be0db45-527e-43de-9906-4ff20d1f7bd8)

- Benefícios:
  - Transparência: Entenda como os dados são transformados.
  -  Compliance: Atenda a regulamentações (ex.: LGPD, GDPR).
 
#### 3. Histórico de Operações (History)
- Auditoria Detalhada: permite conferir o histórico de mudanças ocorridas em uma tabela, como o tipo de operação, os parâmetros da operação, o runtime do Databricks usado para criá-la e também um hiperlink para acessar o notebook que a gerou
- Caso de Uso:
  - Investigar quem alterou uma tabela e quando.
  - Reverter mudanças indesejadas usando Time Travel.
  
  ![image](https://github.com/user-attachments/assets/935d6300-c594-4355-b745-48847ad9436b)


#### 4. Segurança Integrada
- Máscara de Dados: Ofuscação de colunas sensíveis.
- Tags: Classificação de dados (financeiro, confidencial).

Com o Unity Catalog, a governança de dados torna-se centralizada, auditável e escalável.

## Referencias:
- https://datalivre.medium.com/joins-em-pyspark-3c1d2773eeb1
- https://delta.io/
- https://docs.databricks.com/aws/en/
- https://learn.microsoft.com/pt-br/azure/databricks/
- https://www.openbrewerydb.org/faq/
