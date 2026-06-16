# Estudo e Avaliação: FSAssetService e Alternativa de Alta Performance

Este documento apresenta uma avaliação técnica do módulo `FSAssetService` atual do OpenSim e propõe uma nova arquitetura para um módulo alternativo focado em performance, segurança e facilidade de manutenção.

---

## 1. Avaliação do FSAssetService Atual

O `FSAssetService` é um sistema de armazenamento de assets baseado em conteúdo (Content-Addressable Storage - CAS), onde os dados são identificados pelo hash SHA256 de seu conteúdo.

### Pontos Fortes
* **Deduplicação:** Assets idênticos ocupam apenas um espaço físico.
* **Desacoplamento:** Metadados (SQL) e dados brutos (Sistema de Arquivos) são separados.
* **Simplicidade:** Utiliza primitivas básicas do sistema operacional.

### Pontos Fracos e Limitações
1. **Escalabilidade do Sistema de Arquivos (O problema dos "milhões de arquivos"):**
   * Sistemas de arquivos degradam a performance com milhões de pequenos arquivos. Operações de `listing` e `backup` tornam-se extremamente lentas.
2. **Performance de I/O e Compressão:**
   * O uso de `GZipStream` é CPU-intensive. O overhead para assets pequenos é alto.
3. **Segurança e Integridade:**
   * **Falta de Verificação Ativa:** O serviço não valida o hash no momento da leitura (`Get`), permitindo a entrega de dados corrompidos (bit rot).
4. **Dificuldade de Backup:**
   * Backup de milhões de arquivos individuais é ineficiente.

---

## 2. Proposta: `AdvancedAssetStore` (Novo Módulo)

Propõe-se a criação de um novo módulo focado em resolver os gargalos de escala e segurança.

### Arquitetura de Armazenamento: "PackFiles"
Assets agrupados em arquivos grandes (ex: 512MB). Um índice local mapeiva o `Hash -> (PackFileID, Offset, Length)`.
* **Vantagem:** Reduz o número de arquivos de milhões para milhares. Backup via `rsync` torna-se trivial.

### Melhorias de Performance e Caching
* **Compressão LZ4/Zstd:** Alternativas ultra-rápidas ao GZip.
* **Camada de Cache Híbrida (L1/L2):**
    * **L1 (In-Memory):** Cache LRU local para acesso imediato.
    * **L2 (Opcional - Redis/Memcached):** Camada de cache distribuído para ambientes de Grid, permitindo que múltiplos servidores compartilhem assets sem repetir I/O de disco.
* **Zero-Copy I/O:** Uso de `MemoryMappedFiles` para leitura eficiente.

### Gestão e Interoperabilidade
O módulo suporta as seguintes funcionalidades via console (prefixo `aas`):
* **Interoperabilidade FS:** `aas export-legacy <path>` e `aas import-legacy <path>`.
* **Gestão Individual:** `aas export-asset <ID> <path>` e `aas import-asset <path> <type> <name>`.

### Busca e Segurança
* **Busca por Conteúdo:** `aas search-content <string>` para localizar padrões dentro dos assets.
* **Verificação de Integridade:** `aas verify` para validação total do repositório.
* **Criptografia em Repouso:** Suporte a criptografia AES no nível do PackFile.

---

## 3. Comparativo Técnico

| Característica | FSAssetService (Atual) | AdvancedAssetStore (Proposto) |
| :--- | :--- | :--- |
| **Estrutura de Disco** | Milhões de arquivos .gz | Poucos arquivos de Pacote (Blobs) |
| **Velocidade de Backup** | Muito Baixa | Altíssima (Throughput sequencial) |
| **Algoritmo de Compressão** | GZip (Lento) | LZ4 / Zstd (Ultra Rápido) |
| **Verificação de Integridade** | Nenhuma (reativa) | Ativa (Hash check no Get) |
| **Camada de Cache** | Apenas Local | Local + Distribuído (Redis/Memcached) |
| **Busca por Conteúdo** | Não disponível | Integrada (Individual e Global) |

---

## 4. Próximos Passos para Implementação

1. **Definição do Formato do PackFile:** Cabeçalho e estrutura de blobs.
2. **Implementação do Índice:** Integração com SQLite ou RocksDB para mapeamento de offsets.
3. **Refinamento do Command Handler:** Finalizar a lógica de busca e verificação.
4. **Driver de Cache:** Criar o provider opcional para Redis.
