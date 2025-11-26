import os
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from src.etl.db.mongodb.mongo_finder import get_mongo_host, build_mongo_uri, list_mongo_containers

# Load environment variables
load_dotenv()

# MongoDB configuration
MONGO_PORT = os.getenv("MONGO_PORT")
MONGO_CONTAINER_NAME = os.getenv("MONGO_CONTAINER_NAME")

MONGO_USERNAME = os.getenv("MONGO_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")

# Students database configuration
STUDENTS_DB = os.getenv("STUDENTS_DB")
STUDENTS_MESSAGES_COLLECTION = os.getenv("STUDENTS_MESSAGES_COLLECTION")

# Sales database configuration
SALES_DB = os.getenv("SALES_DB")
SALES_LAST_RUN_COLLECTION = os.getenv("SALES_LAST_RUN_COLLECTION")

# Validate required environment variables
required_vars = {
    "STUDENTS_DB": STUDENTS_DB,
    "STUDENTS_MESSAGES_COLLECTION": STUDENTS_MESSAGES_COLLECTION,
    "SALES_DB": SALES_DB,
    "SALES_LAST_RUN_COLLECTION": SALES_LAST_RUN_COLLECTION
}

missing_vars = [var for var, value in required_vars.items() if not value]
if missing_vars:
    error_msg = f"""
{'='*60}
CONFIGURATION ERROR - Missing Environment Variables
{'='*60}
The following required environment variables are not set:
{chr(10).join(f'  - {var}' for var in missing_vars)}

Please check your .env file and ensure it contains:
  STUDENTS_DB=students_db
  STUDENTS_MESSAGES_COLLECTION=messages
  SALES_DB=sales_db
  SALES_LAST_RUN_COLLECTION=last_run_timestamp

Current .env location: {os.path.abspath('.env')}
{'='*60}
"""
    raise ValueError(error_msg)

class MongoDBConnection:
    """Handler for MongoDB connection and setup"""
    
    _instance = None
    _client = None
    _students_db = None
    _sales_db = None
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
            print(f"Attempting to connect to MongoDB...")
            print(f"   Host: {self._host}:{MONGO_PORT}")
            print(f"   Students Database: {STUDENTS_DB}")
            print(f"   Sales Database: {SALES_DB}")
            
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
            
            # Setup both databases
            self._students_db = self._client[STUDENTS_DB]
            self._sales_db = self._client[SALES_DB]
            print(f"✓ Using databases: {STUDENTS_DB}, {SALES_DB}")
            
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
            # Get collection references from different databases
            students_messages_collection = self._students_db[STUDENTS_MESSAGES_COLLECTION]
            sales_last_run_collection = self._sales_db[SALES_LAST_RUN_COLLECTION]
            
            print(f"Setting up collections...")
            
            # Create indexes for students/messages collection
            self._create_indexes(students_messages_collection, STUDENTS_MESSAGES_COLLECTION, "students")
            
            # Create indexes for sales last run collection
            self._create_last_run_indexes(sales_last_run_collection, SALES_LAST_RUN_COLLECTION)
            
            print(f"✓ Collections and indexes setup complete")
            
        except Exception as e:
            print(f"Warning: Could not setup collections: {e}")
    
    def _create_indexes(self, collection, collection_name, collection_type="general"):
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
            
            # Index on lesson for filtering (if students collection)
            if collection_type == "students":
                collection.create_index([("lesson", ASCENDING)], name="lesson_idx")
            
            # Index on name for searching
            collection.create_index([("name", ASCENDING)], name="name_idx")
            
            print(f"   ✓ Created indexes for {collection_name} ({collection_type})")
            
        except Exception as e:
            print(f"   ⚠ Could not create indexes for {collection_name}: {e}")
    
    def _create_last_run_indexes(self, collection, collection_name):
        """Create indexes for last run timestamp collection"""
        try:
            # Index on identifier (job name or process name)
            collection.create_index([("identifier", ASCENDING)], name="identifier_idx", unique=True)
            
            # Index on last_run_timestamp
            collection.create_index([("last_run_timestamp", ASCENDING)], name="last_run_timestamp_idx")
            
            print(f"   ✓ Created indexes for {collection_name} (tracking)")
            
        except Exception as e:
            print(f"   ⚠ Could not create indexes for {collection_name}: {e}")
    
    def get_students_database(self):
        """Get Students MongoDB database instance"""
        if self._students_db is None:
            self._connect()
        return self._students_db
    
    def get_sales_database(self):
        """Get Sales MongoDB database instance"""
        if self._sales_db is None:
            self._connect()
        return self._sales_db
    
    def get_collection(self, database_name, collection_name):
        """Get a specific collection from a database"""
        if database_name == "students":
            return self.get_students_database()[collection_name]
        elif database_name == "sales":
            return self.get_sales_database()[collection_name]
        else:
            raise ValueError(f"Unknown database: {database_name}")
    
    def get_students_messages_collection(self):
        """Get students/messages collection"""
        return self.get_students_database()[STUDENTS_MESSAGES_COLLECTION]
    
    def get_messages_collection(self):
        """Get students/messages collection (alias for backward compatibility)"""
        return self.get_students_messages_collection()
    
    def get_sales_last_run_collection(self):
        """Get sales last run timestamp collection"""
        return self.get_sales_database()[SALES_LAST_RUN_COLLECTION]
    
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
            "students_database": STUDENTS_DB,
            "students_messages_collection": STUDENTS_MESSAGES_COLLECTION,
            "sales_database": SALES_DB,
            "sales_last_run_collection": SALES_LAST_RUN_COLLECTION,
            "is_connected": self.test_connection()
        }
    
    def list_collections(self):
        """List all collections in both databases"""
        try:
            print(f"\n📚 Collections in {STUDENTS_DB}:")
            students_collections = self._students_db.list_collection_names()
            for col in students_collections:
                count = self._students_db[col].count_documents({})
                print(f"   - {col}: {count} documents")
            
            print(f"\n💼 Collections in {SALES_DB}:")
            sales_collections = self._sales_db.list_collection_names()
            for col in sales_collections:
                count = self._sales_db[col].count_documents({})
                print(f"   - {col}: {count} documents")
            
            return {
                "students_db": students_collections,
                "sales_db": sales_collections
            }
        except Exception as e:
            print(f"Error listing collections: {e}")
            return {}
    
    def close(self):
        """Close MongoDB connection"""
        if self._client:
            self._client.close()
            self._client = None
            self._students_db = None
            self._sales_db = None
            print("Closed MongoDB connection")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


def get_mongo_connection():
    """Get MongoDB connection instance"""
    return MongoDBConnection()