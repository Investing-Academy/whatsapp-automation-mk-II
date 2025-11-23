import os
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

# Load environment variables
load_dotenv()

# MongoDB configuration
MONGO_PORT = os.getenv("MONGO_PORT")
MONGO_CONTAINER_NAME = os.getenv("MONGO_CONTAINER_NAME")

MONGO_USERNAME = os.getenv("MONGO_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")

MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

class MongoDBConnection:
    """Handler for MongoDB connection and setup"""
    
    _instance = None
    _client = None
    _db = None
    _host = None
    
    def __new__(cls):
        """Singleton pattern to ensure single connection"""
        if cls._instance is None:
            cls._instance = super(MongoDBConnection, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize MongoDB connection if not already connected"""
        if self._client is None:
            self._connect()
    
    def _connect(self):
        """Establish connection to MongoDB"""
        # Auto-detect MongoDB host
        self._host = get_mongo_host()
        mongo_uri = build_mongo_uri(self._host)
        
        try:
            print(f"\nAttempting to connect to MongoDB...")
            print(f"   Host: {self._host}:{MONGO_PORT}")
            print(f"   Database: {MONGO_DB_NAME}")
            
            # Create client with timeout settings
            self._client = MongoClient(
                mongo_uri,
                serverSelectionTimeoutMS=5000,  # 5 second timeout
                connectTimeoutMS=5000,
                socketTimeoutMS=5000
            )
            
            # Test connection
            self._client.admin.command('ping')
            print(f"✓ Successfully connected to MongoDB!")
            
            self._db = self._client[MONGO_DB_NAME]
            print(f"✓ Using database: {MONGO_DB_NAME}")
            
            # Setup collections and indexes
            self._setup_collections()
            
        except ServerSelectionTimeoutError:
            print(f"Could not connect to MongoDB at {self._host}:{MONGO_PORT}")
            print(f"Trying to find MongoDB containers...")
            list_mongo_containers()
            raise
        except ConnectionFailure as e:
            print(f"Failed to connect to MongoDB: {e}")
            raise
        except Exception as e:
            print(f"Error during MongoDB setup: {e}")
            raise
    
    def _setup_collections(self):
        """Setup collections and create indexes"""
        try:
            # Get collection references
            messages_collection = self._db[COLLECTION_NAME]
            
            print(f"\nSetting up collections...")
            
            # Create indexes for messages collection
            self._create_indexes(messages_collection, COLLECTION_NAME)
            
            print(f"✓ Collections and indexes setup complete")
            
        except Exception as e:
            print(f"⚠️ Warning: Could not setup collections: {e}")
    
    def _create_indexes(self, collection, collection_name):
        """Create indexes for a collection"""
        try:
            # Index on phone_number for fast lookups
            collection.create_index([("phone_number", ASCENDING)], name="phone_number_idx")
            
            # Index on timestamp for sorting
            collection.create_index([("timestamp", ASCENDING)], name="timestamp_idx")
            
            # Compound index for phone + timestamp
            collection.create_index([
                ("phone_number", ASCENDING),
                ("timestamp", ASCENDING)
            ], name="phone_timestamp_idx")
            
            # Index on lesson for filtering
            collection.create_index([("lesson", ASCENDING)], name="lesson_idx")
            
            # Index on name for searching
            collection.create_index([("name", ASCENDING)], name="name_idx")
            
            print(f"  ✓ Created indexes for {collection_name}")
            
        except Exception as e:
            print(f"  ⚠️ Could not create indexes for {collection_name}: {e}")
    
    def get_database(self):
        """Get MongoDB database instance"""
        if self._db is None:
            self._connect()
        return self._db
    
    def get_collection(self, collection_name):
        """Get a specific collection"""
        return self.get_database()[collection_name]
    
    def get_messages_collection(self):
        """Get sent messages collection"""
        return self.get_collection(COLLECTION_NAME)
    
    def test_connection(self):
        """Test if connection is alive"""
        try:
            self._client.admin.command('ping')
            return True
        except Exception:
            return False
    
    def get_connection_info(self):
        """Get connection information"""
        return {
            "host": self._host,
            "port": MONGO_PORT,
            "database": MONGO_DB_NAME,
            "messages_collection": COLLECTION_NAME,
            "is_connected": self.test_connection()
        }
    
    def list_collections(self):
        """List all collections in the database"""
        try:
            collections = self._db.list_collection_names()
            print(f"\nCollections in {MONGO_DB_NAME}:")
            for col in collections:
                count = self._db[col].count_documents({})
                print(f"   - {col}: {count} documents")
            return collections
        except Exception as e:
            print(f"Error listing collections: {e}")
            return []
    
    def close(self):
        """Close MongoDB connection"""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
            print("\n🔌 Closed MongoDB connection")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


def get_mongo_connection():
    """Get MongoDB connection instance"""
    return MongoDBConnection()
