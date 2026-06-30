#!/bin/bash
set -e

# If secrets exist, read them into environment variables
if [ -f "/run/secrets/db_password" ]; then
    export DB_PASSWORD=$(cat /run/secrets/db_password | tr -d '\r\n')
fi

if [ -f "/run/secrets/s3_access_key" ]; then
    export S3_ACCESS_KEY=$(cat /run/secrets/s3_access_key | tr -d '\r\n')
fi

if [ -f "/run/secrets/s3_secret_key" ]; then
    export S3_SECRET_KEY=$(cat /run/secrets/s3_secret_key | tr -d '\r\n')
fi

# ----------------------------------------------------------------------------
# ROBUST CONFIGURATION
# ----------------------------------------------------------------------------
if [ -d "/etc/opensim/config" ]; then
    mkdir -p /app/config
    cp /etc/opensim/config/*.ini /app/config/
fi

# ----------------------------------------------------------------------------
# OPENSIM SIMULATOR CONFIGURATION
# ----------------------------------------------------------------------------
if [ -n "$OPENSIM_REGION_NAME" ]; then
    echo "Creating dynamic region files from environment variables..."
    
    # 1. Create OpenSim.ini
    cat <<EOF > /app/OpenSim.ini
[Startup]
    Include-Architecture = "config-include/GridHypergrid.ini"
    console = "Basic"
EOF

    # 2. Create GridCommon.ini
    mkdir -p /app/config-include
    cat <<EOF > /app/config-include/GridCommon.ini
[DatabaseService]
    StorageProvider = "OpenSim.Data.MySQL.dll"
    ConnectionString = "Data Source=db;Port=3307;Database=${OPENSIM_DB_NAME};User ID=${OPENSIM_DB_USER};Password=${DB_PASSWORD};Old Guids=true;SslMode=None;"

[AssetService]
    DefaultAssetLoader = "OpenSim.Framework.AssetLoader.Filesystem.dll"
    AssetLoaderArgs = "assets/AssetSets.xml"
    AssetServerURI = "https://asset.oligrid.net"

[InventoryService]
    InventoryServerURI = "https://inventory.oligrid.net"

[GridInfo]
    GridInfoURI = "https://grid.oligrid.net"

[GridService]
    GridServerURI = "https://private-grid.oligrid.net"
    Gatekeeper = "https://hg.oligrid.net"

[EstateService]
    EstateServerURI = "https://private-user.oligrid.net"

[AvatarService]
    AvatarServerURI = "https://private-user.oligrid.net"

[PresenceService]
    PresenceServerURI = "https://private-user.oligrid.net"

[UserAccountService]
    UserAccountServerURI = "https://private-user.oligrid.net"

[GridUserService]
    GridUserServerURI = "https://private-user.oligrid.net"

[FriendsService]
    FriendsServerURI = "https://private-user.oligrid.net"

[Messaging]
    Gatekeeper = "https://hg.oligrid.net"
EOF

    # 3. Create Regions.ini
    mkdir -p /app/Regions
    cat <<EOF > /app/Regions/Regions.ini
[${OPENSIM_REGION_NAME}]
    RegionUUID = "${OPENSIM_REGION_UUID}"
    Location = "${OPENSIM_REGION_LOC}"
    InternalAddress = "0.0.0.0"
    InternalPort = 9000
    AllowAlternatePorts = False
    ExternalAddress = "\${EXTERNAL_IP}"
EOF

elif [ -d "/etc/opensim/simulator" ]; then
    echo "Configuring OpenSim Region Simulator from config mounts..."
    
    # Copy main simulator ini
    if [ -f "/etc/opensim/simulator/OpenSim.ini" ]; then
        cp /etc/opensim/simulator/OpenSim.ini /app/OpenSim.ini
    fi
    
    # Copy GridCommon config
    if [ -f "/etc/opensim/simulator/GridCommon.ini" ]; then
        mkdir -p /app/config-include
        cp /etc/opensim/simulator/GridCommon.ini /app/config-include/GridCommon.ini
    fi
    
    # Copy Regions definition
    if [ -f "/etc/opensim/simulator/Regions.ini" ]; then
        mkdir -p /app/Regions
        cp /etc/opensim/simulator/Regions.ini /app/Regions/Regions.ini
    fi
    
    # Dynamic External IP resolution for simulator UDP packets
    if [ -z "$EXTERNAL_IP" ] || [ "$EXTERNAL_IP" = "AUTO" ]; then
        echo "Auto-detecting host external IP..."
        # Try fetching public IP via standard web APIs
        RESOLVED_IP=$(curl -s --max-time 5 https://ipinfo.io/ip || curl -s --max-time 5 https://api.ipify.org || echo "")
        
        # Fallback to local node interface IP if offline or blocked
        if [ -z "$RESOLVED_IP" ]; then
            RESOLVED_IP=$(hostname -i | awk '{print $1}')
            echo "Web IP lookup failed, using local interface IP: $RESOLVED_IP"
        else
            echo "Web IP lookup succeeded: $RESOLVED_IP"
        fi
        
        export EXTERNAL_IP="$RESOLVED_IP"
    fi
    
    echo "Setting simulator ExternalAddress to: $EXTERNAL_IP"
fi

# ----------------------------------------------------------------------------
# PLACEHOLDERS SUBSTITUTION
# ----------------------------------------------------------------------------
# Find all .ini files in /app and subfolders to perform substitutions
find /app -name "*.ini" -type f | while read -r f; do
    if [ -f "$f" ]; then
        # Replace password, S3 credentials, and external IP placeholders
        sed -i "s/\${DB_PASSWORD}/${DB_PASSWORD}/g" "$f" 2>/dev/null || true
        sed -i "s/\${S3_ACCESS_KEY}/${S3_ACCESS_KEY}/g" "$f" 2>/dev/null || true
        sed -i "s/\${S3_SECRET_KEY}/${S3_SECRET_KEY}/g" "$f" 2>/dev/null || true
        sed -i "s/\${EXTERNAL_IP}/${EXTERNAL_IP}/g" "$f" 2>/dev/null || true
    fi
done

# Run the command passed to docker
exec "$@"
