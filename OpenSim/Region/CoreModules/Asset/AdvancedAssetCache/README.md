# AdvancedAssetCache (AAC)

O `AdvancedAssetCache` é um módulo de cache de assets para o OpenSim que utiliza a mesma arquitetura de **PackFiles** do `AdvancedAssetService`.

## Vantagens em relação ao FlotsamAssetCache:
1.  **Redução de Arquivos:** Agrupa assets em arquivos grandes (256MB), evitando milhões de pequenos arquivos no disco.
2.  **Índice Rápido:** Utiliza SQLite para mapeamento instantâneo de UUID -> Offset.
3.  **Deduplicação Nativa:** Assets idênticos (mesmo hash) ocupam apenas um espaço no cache, mesmo com UUIDs diferentes.
4.  **Limpeza Eficiente (LRU):** Quando o limite de tamanho é atingido, o cache remove pacotes inteiros, o que é muito mais rápido do que deletar milhares de arquivos individuais.

## Como Habilitar

No seu arquivo `OpenSim.ini` (ou `StandaloneCommon.ini`), altere a seção `[Modules]` e `[AssetCache]`:

```ini
[Modules]
    AssetCaching = "AdvancedAssetCache"

[AssetCache]
    ;; Diretório onde os pacotes de cache serão salvos
    CacheDirectory = "assetcache_aac"
    
    ;; Tamanho máximo do cache em Megabytes (ex: 2048 = 2GB)
    MaxCacheSize = 2048
```

## Estrutura de Arquivos
*   `cache_index.db`: Banco de dados SQLite com os metadados.
*   `cache_pack_N.bin`: Arquivos binários contendo os dados dos assets.
