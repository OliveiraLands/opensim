# AdvancedAssetService: Sistema de Armazenamento de Alta Performance

Este documento descreve a implementação e o funcionamento do `AdvancedAssetService`, um módulo de armazenamento de assets para OpenSim focado em escalabilidade, performance e integridade.

## 1. Arquitetura

O `AdvancedAssetService` utiliza uma abordagem de **Content-Addressable Storage (CAS)** baseada em **PackFiles**, resolvendo o problema de milhões de pequenos arquivos no sistema operacional.

### Componentes Principais:
* **PackFiles (`pack_N.bin`):** Arquivos grandes (default 512MB) que agrupam múltiplos assets sequencialmente. Isso reduz drasticamente o número de handles de arquivo e simplifica backups.
* **Índice SQLite (`index.db`):** Armazena o mapeamento de `Hash -> (PackFileID, Offset, Length)` e o mapeamento de metadados `UUID -> (Hash, Type, Name, Created)`.
* **Deduplicação Nativa:** Assets com o mesmo conteúdo (mesmo hash SHA256) são armazenados apenas uma vez fisicamente.
* **Background Writes:** Utiliza uma fila de escrita assíncrona com `BlockingCollection` para não bloquear as threads de rede durante o armazenamento de assets.

## 2. Funcionalidades Implementadas

O serviço pode ser gerenciado através do console do OpenSim usando o prefixo `aas`:

### Gestão de Assets
* `aas import-asset <path> <type> <name>`: Importa um arquivo local para o sistema de assets.
* `aas export-asset <ID> <path>`: Exporta o conteúdo de um asset para um arquivo local.
* `aas import-legacy <path>`: Importa em massa assets de uma estrutura de pastas no formato `FSAssetService` (.gz).
* `aas import-raw <path>`: Importa em massa arquivos raw de assets não comprimidos nomeados por UUID.
* `aas export-legacy <path>`: Exporta todos os assets do serviço para uma estrutura de pastas compatível com `FSAssetService`.

### Manutenção e Busca
* `aas search-content <string>`: Busca assets pelo nome nos metadados.
* `aas verify`: Verifica a integridade física de todos os assets, validando os hashes SHA256.
* `aas rebuild-index`: Reconstrói totalmente o arquivo `index.db` a partir da leitura dos `PackFiles`, permitindo recuperação total em caso de corrupção do banco de dados.
* `aas compare <path>`: Compara os assets locais do AAS com uma pasta externa de assets legado.
* `aas sync-database`: Força a sincronização completa de todos os assets locais pendentes com o banco de dados da grid (Shadow Sync).
* `aas sync-s3`: Força a sincronização imediata de todos os arquivos de pacotes de assets locais com o S3.

## 3. Próximos Passos (Roadmap)

* **Compressão LZ4/Zstd:** Implementar compressão ultra-rápida nos blocos de dados dentro dos PackFiles.
* **Camada de Cache L2 (Redis):** Suporte a cache distribuído para ambientes de Grid.
* **Criptografia em Repouso:** Suporte a criptografia AES nos PackFiles.
* **Zero-Copy I/O:** Otimizar leituras usando `MemoryMappedFiles`.

## 4. Comparativo de Performance

| Característica | FSAssetService (Legacy) | AdvancedAssetService (AAS) |
| :--- | :--- | :--- |
| **Estrutura de Disco** | Milhões de arquivos .gz | Poucos arquivos de Pacote (.bin) |
| **Performance de Escrita** | Síncrona (Disk I/O Bound) | Assíncrona (Buffered/Queue) |
| **Indexação** | SQL (UUID -> Hash) | SQL (UUID -> Hash + Hash -> Pack) |
| **Integridade** | Reativa | Ativa (Hash check) |
| **Busca** | Limitada | Integrada (Metadata) |
| **Recuperação** | Difícil (Milhões de arquivos) | Fácil (Rebuild via PackFiles) |
