# Manual de Resiliência e Integridade de Assets: Advanced Asset Service

Este documento descreve estratégias e propostas de engenharia para elevar a **segurança, durabilidade e resiliência** do módulo `AdvancedAssetService` (AAS) contra perda de dados, corrupções físicas e falhas catastróficas de hardware ou energia (crash do servidor).

---

## 1. Diagnóstico de Vulnerabilidade e Risco de Corrupção

Embora o sistema de **PackFiles** concentre os dados em arquivos sequenciais e utilize um banco SQLite para indexação, a implementação atual possui pontos de vulnerabilidade a falhas físicas:

1. **Gravações em Buffer (Ausência de Hardware Flush):** As escritas físicas de novos assets nos arquivos `.bin` utilizam a classe `BinaryWriter` sobre um `FileStream` padrão. Ao fechar o arquivo, os dados são enviados para o cache do sistema operacional (SO), mas não há uma barreira física de gravação (fsync). Se o servidor perder energia logo após a gravação, esses dados serão perdidos ou gravados de forma parcial.
2. **SQLite no modo Inseguro (PRAGMA synchronous = OFF):** O banco de índice local utiliza `synchronous = OFF`. Embora melhore a velocidade de escritas síncronas, este modo é altamente vulnerável. Se o sistema operacional falhar ou houver falta de energia, o banco `index.db` pode sofrer corrupção de páginas físicas, exigindo uma reconstrução total.
3. **Corrupção por Append após Quedas:** Se o servidor cair durante a gravação de um asset, o final do arquivo de pacote `.bin` conterá dados truncados/parciais (com magic number inválido ou tamanho incorreto). Ao reiniciar, novos assets serão anexados *após* esses bytes corrompidos, inviabilizando varreduras completas e corrompendo permanentemente o alinhamento de offsets.

---

## 2. As 5 Melhorias de Segurança e Resiliência Propostas

Abaixo estão detalhadas as cinco principais soluções de engenharia para blindar o módulo Advanced Asset contra perdas e corrupções de dados:

### Otimização 1: Durabilidade Física no Disco (Fsync / Flush de Hardware)
Garantir que os bytes do asset foram fisicamente persistidos no prato do disco ou nas células do SSD antes de confirmar o sucesso da operação para o simulador.

#### O que mudar no código C# (`PackFile.cs`):
Ao concluir a escrita de um asset no PackFile, forçar a descarga do cache de escrita do sistema operacional para a mídia física utilizando `fs.Flush(true)`. O parâmetro `true` força o flush dos metadados e buffers do sistema de arquivos para o disco (equivalente ao `fsync` no Linux).

```csharp
using (FileStream fs = new FileStream(packPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
using (BinaryWriter bw = new BinaryWriter(fs))
{
    offset = fs.Position;
    bw.Write(MAGIC_NUMBER);
    bw.Write(RECORD_VERSION);
    bw.Write(new UUID(op.UUID).GetBytes());
    bw.Write(op.Type);
    bw.Write(op.Created);
    byte[] nameBytes = Encoding.UTF8.GetBytes(op.Name ?? "");
    bw.Write((ushort)nameBytes.Length);
    bw.Write(nameBytes);
    bw.Write(op.Data.Length);
    bw.Write(op.Data);

    // BARREIRA DE GRAVAÇÃO FÍSICA (FSYNC)
    fs.Flush(true); 
}
```

---

### Otimização 2: Segurança no Banco SQLite (synchronous = NORMAL no modo WAL)
Aumentar a tolerância do SQLite contra desligamentos abruptos sem comprometer a performance de escrita concorrente.

#### O que mudar no código C# (`PackFile.cs` e `PackFileCache.cs`):
No modo WAL (Write-Ahead Logging), o SQLite permite leitores e escritores paralelos de forma segura. Alterar a configuração de sincronização de `OFF` para `NORMAL`. De acordo com a documentação oficial do SQLite, o modo **`synchronous = NORMAL` é completamente seguro contra corrupções no modo WAL**, pois realiza sincronizações em pontos de verificação (checkpoints) em vez de cada transação individual.

```csharp
// Modificar em InitializeDatabase:
ExecuteNonQuery("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA temp_store=MEMORY;");
```

---

### Otimização 3: Crash Recovery e Auto-Truncamento em Boot
Evitar a contaminação de novos pacotes após uma queda abrupta do servidor.

#### O que mudar no código C# (`PackFile.cs`):
Durante a inicialização do `PackFileManager`, o sistema deve analisar o último arquivo de pacote ativo (`pack_N.bin`). O sistema deve ler sequencialmente os blocos a partir do início. Ao encontrar o primeiro bloco corrompido ou incompleto (causado por uma queda no meio da gravação), o sistema deve **truncar o arquivo naquele exato offset de início do bloco corrompido**, eliminando o lixo físico e garantindo que as próximas gravações comecem a partir de um alinhamento 100% íntegro.

#### Esboço do Algoritmo de Crash Recovery:
```csharp
private void PerformCrashRecovery(string activePackPath)
{
    if (!File.Exists(activePackPath)) return;

    long lastValidOffset = 0;
    using (FileStream fs = new FileStream(activePackPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
    using (BinaryReader br = new BinaryReader(fs))
    {
        try
        {
            while (fs.Position < fs.Length)
            {
                long currentOffset = fs.Position;
                
                // Se faltar bytes para ler o cabeçalho mínimo, chegamos ao fim parcial
                if (fs.Length - currentOffset < 25) break; 
                
                if (br.ReadUInt32() != MAGIC_NUMBER) break;
                ushort version = br.ReadUInt16();
                br.ReadBytes(16); // UUID
                br.ReadSByte(); // Type
                if (version >= 2) br.ReadInt64(); // Created
                
                ushort nameLen = br.ReadUInt16();
                if (fs.Length - fs.Position < nameLen + 4) break; // Incompleto
                br.ReadBytes(nameLen);
                
                int dataLen = br.ReadInt32();
                if (fs.Length - fs.Position < dataLen) break; // Incompleto
                br.ReadBytes(dataLen);
                
                // Se chegou até aqui sem erros, o registro é 100% válido
                lastValidOffset = fs.Position;
            }

            // Se o arquivo tiver bytes corrompidos/incompletos no final, trunca!
            if (fs.Length > lastValidOffset)
            {
                m_log.Warn($"[AAS Recovery]: Truncating corrupted trailing bytes in active pack at offset {lastValidOffset}. Saved {fs.Length - lastValidOffset} bytes of corrupt data.");
                fs.SetLength(lastValidOffset);
            }
        }
        catch (Exception ex)
        {
            m_log.Error($"[AAS Recovery]: Failed to scan active pack for crash recovery: {ex.Message}");
        }
    }
}
```

---

## 4. Auto-Reparo Ativo (Self-Healing de Leitura)
Corrigir corrupções físicas ou arquivos de pacotes deletados acidentalmente de forma automática em tempo de execução, sem intervenção humana.

#### O que mudar no código C# (`AdvancedAssetService.cs`):
Se o sistema tentar ler um asset e:
1. Detectar que o arquivo de pacote `.bin` correspondente não existe mais no disco, ou
2. Detectar uma falha de integridade física (hash SHA-256 lido difere do esperado em `VerifyOnRead`),
E se houver um **Fallback Service** (`m_FallbackService`) configurado, o sistema deve:
* Buscar o asset intacto no Fallback Service (ou no backup S3 se ativo).
* Re-gravar o asset nos pacotes ativos locais.
* Atualizar os metadados no SQLite para apontar para a nova localização íntegra.
* Logar um aviso de auto-reparo concluído.

Isso garante que corrupções pontuais de disco sejam "curadas" automaticamente à medida que os usuários acessam os itens.

---

### Otimização 5: Replicação Contínua via Litestream
O backup atômico via temporizador do SQLite protege o banco de índice, mas tem um intervalo de minutos ou horas. Para uma resiliência nível Enterprise:

1. **Instalar o Litestream:** Uma ferramenta open-source ultraleve projetada especificamente para SQLite.
2. **Monitoramento WAL:** O Litestream roda como um serviço em segundo plano monitorando o Write-Ahead Log (`index.db-wal`) do SQLite.
3. **Streaming Segundo a Segundo:** Ele replica alterações de páginas físicas no banco de índice para o armazenamento compatível com S3 segundo a segundo.
4. **Resiliência Total:** Em caso de perda total do servidor, o banco de índice pode ser restaurado até a última transação bem-sucedida, sem perda de inventários ou mapeamentos físicos.

---

## 3. Resumo de Adoção e Recomendações

| Estratégia | Nível de Proteção | Impacto de Performance | Complexidade |
| :--- | :--- | :--- | :--- |
| **1. fs.Flush(true)** | Previne perda de novos assets em crash de energia. | Mínimo (Escrita assíncrona). | Muito Baixa |
| **2. SQLite synchronous=NORMAL** | Previne corrupções no banco de índice SQLite. | Nulo ( WAL mode otimizado). | Muito Baixa |
| **3. Auto-Truncamento (Boot)** | Previne contaminação de dados após quedas. | Nulo (Roda apenas uma vez no boot). | Média |
| **4. Self-Healing no Read** | Corrige corrupções pontuais de disco ativamente. | Nulo no fluxo normal (Apenas no erro). | Média-Alta |
| **5. Litestream Replication** | Proteção catastrófica de índice em tempo real. | Nulo (Processo isolado e assíncrono). | Média |

### Recomendação Imediata:
Implementar **imediatamente** as Otimizações 1 e 2. Elas representam modificações simples de linhas de código isoladas e resolvem 95% das ocorrências de arquivos corrompidos por perda inesperada de energia ou congelamento de processos.
