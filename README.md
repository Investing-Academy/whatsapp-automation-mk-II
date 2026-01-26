# WhatsApp ETL System 📊

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Selenium](https://img.shields.io/badge/Selenium-43B02A?style=for-the-badge&logo=selenium&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-47A248?style=for-the-badge&logo=mongodb&logoColor=white)
![Google Sheets](https://img.shields.io/badge/Google%20Sheets-34A853?style=for-the-badge&logo=google-sheets&logoColor=white)

A powerful ETL (Extract, Transform, Load) pipeline that automates data synchronization between WhatsApp groups, Google Sheets, and MongoDB to streamline sales tracking and student management workflows.

## 🎯 Overview

This project processes messages from two WhatsApp groups and transforms them into actionable data:
- **Sales Group**: Automatically updates Google Sheets when sales matches are detected
- **Students Group**: Maintains a MongoDB database tracking student progress with real-time dashboard visualization

## ✨ Features

### Sales Pipeline
- **Real-time Message Extraction**: Monitors WhatsApp sales group for new messages
- **Intelligent Matching**: Identifies sales-related data using pattern matching
- **Automated Updates**: Syncs matched data directly to Google Sheets for the sales team
- **Seamless Integration**: No manual data entry required

### Student Management System
- **Comprehensive Tracking**: MongoDB document per student tracking practices and lessons
- **Google Sheets Integration**: Syncs with student management platform
- **Bi-directional Updates**: 
  - Pulls student data from Google Sheets
  - Pushes practice updates back to Sheets
- **Teacher Dashboard**: Visual interface for monitoring student progress ([Dashboard Repository](https://github.com/NVB20/dashboard_mk2))
- **Performance Insights**: Identifies students who need additional support

## 🏗️ Architecture

```
WhatsApp Groups
    ├── Sales Group → ETL Pipeline → Google Sheets (Sales Team)
    └── Students Group → ETL Pipeline → MongoDB → Dashboard
                              ↕
                        Google Sheets (Teachers + Lessons)
```

## 🔧 Technology Stack

- **Automation**: Selenium WebDriver for WhatsApp Web interaction
- **Database**: MongoDB for student document storage
- **Spreadsheet Integration**: Google Sheets API
- **Data Extraction**: WhatsApp message parsing via Selenium
- **Data Storage**: 
  - Google Sheets (Sales & Student Management)
  - MongoDB (Student d

## 📊 Data Flow

### Sales Workflow
1. Extract messages from WhatsApp sales group
2. Transform and match sales data patterns
3. Load matched records into Google Sheets
4. Sales team receives real-time updates

### Student Workflow
1. Extract messages from WhatsApp students group
2. Pull existing student data from Google Sheets
3. Create/update MongoDB documents with practice and lesson data
4. Push new practice records back to Google Sheets
5. Teachers monitor progress via dashboard and spreadsheet

## 🚀 Getting Started

### Prerequisites
- Python 3.x
- MongoDB instance
- Google Sheets API credentials
- WhatsApp account
- Docker (for containerized deployment)

### Deployment Options

#### Option 1: Docker (Recommended)

**For Mac (M1/M2/M3 or Intel):**
```bash
# Quick start
cp .env.exemple .env
# Edit .env with your credentials

mkdir -p whatsapp_session secrets logs
cp /path/to/credentials.json secrets/

# Build and run
docker-compose -f docker-compose.mac.yml build
docker-compose -f docker-compose.mac.yml up -d
```

See [DOCKER_MAC_QUICKSTART.md](DOCKER_MAC_QUICKSTART.md) for complete Mac deployment guide.

**For Linux:**
```bash
# Use original Dockerfile
docker-compose up -d
```

#### Option 2: Local Development
```bash
# Clone the repository
git clone <your-repo-url>
cd mk2

# Install dependencies
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure environment variables
cp .env.exemple .env
# Edit .env with your credentials

# Run directly
python main.py

# Or with scheduler
python scheduler.py --interval 3600
```

### Configuration
Set up the following in your `.env` file:
- `STUDENTS_GROUP` - WhatsApp student group name
- `SALES_TEAM_GROUP` - WhatsApp sales group name
- `SHEET_ID` - Google Sheets ID for student data
- `CREDENTIALS_FILE` - Path to Google API credentials
- `MESSAGE_COUNT` - Number of messages to read per run (default: 50)
- `ETL_INTERVAL` - Seconds between ETL runs (default: 7200)

### Documentation
- **[claude.md](claude.md)** - Complete codebase documentation
- **[MAC_DEPLOYMENT.md](MAC_DEPLOYMENT.md)** - Mac deployment guide
- **[DOCKER_MAC_QUICKSTART.md](DOCKER_MAC_QUICKSTART.md)** - Quick reference for Mac
- **[MAC_DOCKER_CHANGES.md](MAC_DOCKER_CHANGES.md)** - Technical details of Mac compatibility

## 📈 Use Cases

- **Sales Team**: Track sales conversations and opportunities without manual logging
- **Teachers**: Monitor which students are practicing regularly and identify those needing encouragement
- **Management**: Overview of both sales pipeline and student engagement metrics

## 🔗 Related Projects

- [Student Dashboard](https://github.com/NVB20/dashboard_mk2) - Real-time visualization of student progress


**Note**: Ensure proper permissions and privacy compliance when processing WhatsApp messages and personal student data.