rem # Ativa o log de persistência em disco (Append-Only File)
docker run -d --name garnet -p 6379:6379  -v .\garnet-data:/data ghcr.io/microsoft/garnet --aof 
