# Advanced Asset Service (AAS) - Manual de Arquitetura, Otimizações e Operações

O `AdvancedAssetService` é um motor de armazenamento de assets de alta performance para OpenSim, projetado especificamente para eliminar gargalos históricos de I/O em disco, otimizar a velocidade de carregamento (REST) e garantir 100% de durabilidade e integridade dos dados na nuvem (S3) e no banco de dados.

---

## 1. Arquitetura do Sistema

O AAS resolve o problema clássico de ter milhões de pequenos arquivos no sistema de arquivos do servidor (como ocorria no FSAssetService legado), agrupando os dados em pacotes consolidados.

### Componentes de Armazenamento:
* **PackFiles (`pack_N.bin`):** Arquivos binários grandes (padrão de 256MB ou 512MB) estruturados em modo *Append-Only* para gravação rápida e sequencial de novos assets. Quando um arquivo atinge o limite de tamanho, ele é fechado (somente leitura) e um novo é criado.
* **Índice SQLite (`index.db`):** Banco relacional embutido de altíssima velocidade que mapeia as relações de indexação local:
  - `UUID -> Hash` (Mapeamento lógico-físico do asset e metadados).
  - `Hash -> (PackFileID, Offset, Length)` (Localização física dos bytes dentro dos arquivos de pacotes).
* **Deduplicação Nativa por Hash:** Assets idênticos (mesmo hash SHA256 de dados binários) são gravados fisicamente apenas uma vez, independentemente de quantas UUIDs ou inventários diferentes apontem para eles.
* **Shadow Sync (Sincronização para a Grid):** Uma thread assíncrona lê a fila local do SQLite e atualiza periodicamente os metadados da tabela central MySQL (`fsassets`), garantindo compatibilidade reversa com ferramentas de terceiros e consultas gerais da Grid.

---

## 2. Otimizações de Performance de Escrita (Restore IAR/OAR)

Durante a restauração de pacotes massivos (como comandos `load iar` ou `load oar`), o simulador envia milhares de assets sequencialmente via REST para o Robust. No AAS legado, o processo sofria lentidão extrema devido a travas físicas de disco. Foram implementadas duas otimizações cruciais:

### A. Ampliação da Fila de Escrita Assíncrona
A fila de buffer (`m_WriteQueue`) no Robust foi ampliada de **5.000 para 20.000 itens**. Isso impede que rajadas massivas de requisições REST travem as threads de processamento HTTP do servidor web.

### B. Write-Ahead Sync por Lote (Fsync Transacional)
Anteriormente, o thread de escrita chamava `fs.Flush(true)` (bloqueio físico do hardware/fsync) para cada asset individual gravado, limitando a velocidade a 50~200 assets/segundo.
* **Nova abordagem:** O `AdvancedAssetService` agora grava os bytes dos assets no arquivo de pacotes utilizando cache de memória do sistema operacional (velocidade de RAM).
* **Sincronização Segura:** Imediatamente antes de chamar o `Commit()` da transação do lote no banco de dados SQLite (lote padrão de 500 assets ou a cada 2 segundos), o AAS abre o arquivo de pacotes ativo e roda um único `fs.Flush(true)`.
* **Resultado:** O tempo de espera da fila REST cai para apenas **3 ms** para lotes de 1.000 assets, com vazão de gravação excedendo **1.300 assets/segundo**, enquanto mantém **100% de durabilidade transacional (ACID)** contra falhas repentinas de energia.

---

## 3. Comandos de Console (`aas`) e Ferramentas

O serviço expõe ferramentas avançadas no console do Robust para diagnóstico e cura de falhas:

### `aas restore-from-log <log_path> <fs_path>`
Analisa logs de erros do simulador (ex: `Robust.log` ou `urma.log`), identifica as UUIDs dos assets que falharam ao carregar e tenta restaurá-los automaticamente a partir de um diretório de arquivos antigo.
* **Auto-Detecção Híbrida de Formatos:** O leitor de arquivos analisa os primeiros bytes (assinatura mágica) de cada arquivo encontrado de forma automática:
  - **Compactados GZip (`0x1F 0x8B`):** Descompacta e importa (Formato padrão do FSAsset legado).
  - **Serializados do .NET (`0x00 0x01`):** Desserializa objetos `AssetBase` nativamente (Formato do cache local Flotsam da região).
  - **Dados Brutos (Raw):** Importa o conteúdo puro (Formato dump comum).
* **Fallback de Metadados:** Se a UUID do log não estiver no banco local, ele consulta a tabela `fsassets` no MySQL para descobrir o hash original e localizar o arquivo na pasta legada.
* **ESC & Resume:** O processo pode ser abortado a qualquer momento pressionando `ESC`. O índice atual é salvo e o comando perguntará se deseja retomar da posição de interrupção na próxima vez que for executado.

### `aas generate-dummies [--verify-data] [--dry-run]`
Cura referências órfãs/corrompidas no inventário dos usuários que apontam para assets inexistentes, eliminando os erros vermelhos do simulador.
* **Design Não-Destrutivo:** Em vez de alterar ou excluir linhas da gigantesca tabela `inventoryitems` do MySQL, ele cria assets substitutos (dummies) válidos gravados sob as **UUIDs originais esperadas**. Isso preserva a referência para caso o administrador encontre o backup físico real no futuro.
* **Dummies Específicos por Tipo:**
  - **LSLText (Script):** Cria um script compilável que avisa o proprietário sobre a perda do asset.
  - **Texture:** Cria uma textura TGA válida em cinza/branco de 2x2.
  - **Notecard:** Cria uma notecard Linden Text v2 legível pelo viewer.
  - **Object:** Cria uma SceneObjectGroup XML válida contendo um cubo vazio descritivo.
  - **Sound:** Cria um arquivo de áudio WAV válido de silêncio absoluto.
  - **Wearable/Clothing/Bodypart:** Gera arquivos de vestuário padrão.
* **Otimização Extrema:** Indexa todos os assets locais em um `HashSet<string>` na memória RAM para realizar pesquisas instantâneas (sub-microssegundos) durante a varredura, emitindo feedback de progresso a cada 50.000 itens para não travar o console.
* **Flag `--verify-data`:** Força o comando a abrir fisicamente e verificar o tamanho em bytes de cada asset em disco. Se houver metadados no banco mas o arquivo físico estiver corrompido ou com zero bytes, ele recria o dummy.
* **Flag `--dry-run`:** Modo de simulação apenas de leitura. Apenas lista o que seria feito no console sem efetuar gravações físicas.

---

## 4. Integração com Hypergrid (HG)

Para garantir que os visitantes vindos de outras grids consigam visualizar texturas, vestuários e rezzar objetos cacheados localmente no Advanced Cache, a configuração do Robust deve ser ajustada.

O módulo public-facing `HGAssetService` atua como um wrapper genérico e deve ser apontado para ler do Advanced Cache nos bastidores:

### Configuração no `Robust.HG.ini` (e `robust.common.ini`):
```ini
[HGAssetService]
    ;; Usa o módulo genérico do Hypergrid
    LocalServiceModule = "OpenSim.Services.HypergridService.dll:HGAssetService"
    
    ;; Define o AdvancedAssetService como backing provider nos bastidores
    BackingService = "OpenSim.Services.AdvancedAssetService.dll:AdvancedAssetService"
    
    UserAccountsService = "OpenSim.Services.UserAccountService.dll:UserAccountService"
    AuthType = None
```

---

## 5. Replicação Cloud Integrada (S3 / R2 / MinIO)

O AAS possui um módulo em segundo plano (`S3BackgroundReplicator`) para contingência e Disaster Recovery.

```ini
[AssetService]
    ;; Configurações de replicação S3
    S3AccessKey = "SUA_CHAVE_DE_ACESSO"
    S3SecretKey = "SUA_CHAVE_SECRETA"
    S3BucketName = "nome-do-bucket"
    S3ServiceUrl = "https://s3.provedor.com"
    S3Region = "us-east-1"
    S3SyncInterval = 30 ; Intervalo em minutos
```

* **Sincronização de PackFiles:** Os arquivos `.bin` fechados e selados são enviados automaticamente ao S3.
* **Snapshot de index.db:** A cada ciclo, o replicador chama a API de Backup do SQLite para gerar uma cópia consistente (`index_backup.db`) livre de travas e faz o upload para o bucket.
* **Recomendação:** Utilizar provedores como Cloudflare R2 ou Wasabi, que possuem **custo zero de download (egress fees)** para facilitar uma restauração completa (Disaster Recovery) sem taxas abusivas de rede.
