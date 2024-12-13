from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Database setup (PostgreSQL)
DB_CONFIG = {
    'dbname': 'galeria',
    'user': 'postgres',
    'password': 'new_password',
    'host': 'localhost',
    'port': '5433'
}
DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def view_tracks():
    """Fetch and display data from the 'tracks' table."""
    session = Session()
    try:
        query = text("SELECT * FROM tracks")
        results = session.execute(query).fetchall()
        
        if not results:
            print("No data found in the 'tracks' table.")
        else:
            print("Data in 'tracks':")
            for row in results:
                print(row)
    except Exception as e:
        print(f"Error fetching data from 'tracks': {e}")
    finally:
        session.close()

if __name__ == "__main__":
    view_tracks()
