import platform
import subprocess
import json
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

MONGO_USERNAME = os.getenv("MONGO_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
MONGO_PORT = os.getenv("MONGO_PORT")

def is_windows():
    return platform.system().lower() == 'windows'


def is_wsl():
    try:
        with open('/proc/version', 'r') as f:
            return 'microsoft' in f.read().lower()
    except:
        return False


def get_docker_container_ip(container_name):
    try:
        # Method 1: Using docker inspect
        result = subprocess.run(
            ['docker', 'inspect', '-f', '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}', container_name],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0 and result.stdout.strip():
            ip = result.stdout.strip()
            print(f"✓ Found container '{container_name}' at IP: {ip}")
            return ip
            
    except subprocess.TimeoutExpired:
        print(f"Docker command timed out")
    except FileNotFoundError:
        print(f"Docker command not found. Is Docker installed?")
    except Exception as e:
        print(f"Error getting container IP: {e}")
    
    return None


def get_mongo_host():  
    # On Windows or WSL, Docker container IPs are not accessible from host
    # Always use localhost with port mapping
    if is_windows():
        print(f"Detected Windows - using localhost (container IPs not accessible from Windows host)")
        return "localhost"
    
    if is_wsl():
        print(f"Detected WSL - using localhost (container IPs not accessible from WSL)")
        return "localhost"
    
    # On Linux, we can try to use container IP
    container_name = os.getenv("MONGO_CONTAINER_NAME")
    if container_name:
        print(f"Detected Linux - attempting to find Docker container: {container_name}")
        container_ip = get_docker_container_ip(container_name)
        if container_ip:
            print(f"Note: Using container IP. If this fails, set MONGO_HOST=localhost in .env")
            return container_ip
    
    # Fallback to localhost
    print(f"Using default: localhost")
    return "localhost"


def list_mongo_containers():
    try:
        # Get all running containers
        result = subprocess.run(
            ['docker', 'ps', '--format', '{{json .}}'],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0:
            containers = []
            for line in result.stdout.strip().split('\n'):
                if line:
                    container = json.loads(line)
                    # Check if it's a MongoDB container
                    if 'mongo' in container.get('Image', '').lower() or 'mongo' in container.get('Names', '').lower():
                        containers.append({
                            'name': container.get('Names'),
                            'id': container.get('ID'),
                            'image': container.get('Image'),
                            'ports': container.get('Ports')
                        })
            
            if containers:
                print(f"\nFound {len(containers)} MongoDB container(s):")
                for c in containers:
                    print(f"   - {c['name']} (ID: {c['id'][:12]}, Image: {c['image']})")
            
            return containers
    except Exception as e:
        print(f"Error listing containers: {e}")
    
    return []


def build_mongo_uri(host):
    """Build MongoDB connection URI"""
    if MONGO_USERNAME and MONGO_PASSWORD:
        # With authentication
        return f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{host}:{MONGO_PORT}/"
    else:
        # Without authentication
        return f"mongodb://{host}:{MONGO_PORT}/"