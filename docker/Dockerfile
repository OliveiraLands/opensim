# Use the official Microsoft .NET 8.0 Runtime image
FROM mcr.microsoft.com/dotnet/runtime:8.0

# Install dependencies (SQLite for local asset index, curl for health checks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy the pre-compiled binaries from local bin directory
COPY ./bin /app

# Copy the entrypoint script and make it executable
COPY ./docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Expose ports for various services
# 8001: Grid/GridInfo
# 8002: LLLogin/Hypergrid
# 8003: Robust Private services
# 8004: AdvCapsServer
EXPOSE 8001 8002 8003 8004

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["dotnet", "Robust.dll"]
