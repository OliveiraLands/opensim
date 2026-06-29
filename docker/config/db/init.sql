-- Create the databases for Robust and OpenSim simulator
CREATE DATABASE IF NOT EXISTS os_oligridnet;
CREATE DATABASE IF NOT EXISTS os_regiao0001;

-- Grant privileges to the user created by MariaDB entrypoint
GRANT ALL PRIVILEGES ON os_oligridnet.* TO 'osuser'@'%';
GRANT ALL PRIVILEGES ON os_regiao0001.* TO 'osuser'@'%';
FLUSH PRIVILEGES;
