Streaming Mental Health Prevalences
This project streams and analyzes global depression and anxiety prevalence data using the Our World in Data Mental Health dataset.
The goal is to simulate how real-time health statistics could be processed and visualized.
📂 Project Structure
Streaming_MentalHealth_Prevalences/
├─ producers/        # Producer scripts (send data to Kafka)
├─ consumers/        # Consumer scripts (process & visualize data)
├─ utils/            # Config, visualization, and helper utilities
├─ data/             # Raw dataset (ignored in Git) + small samples for testing
├─ requirements.txt  # Python dependencies
├─ .env.example      # Example environment variables
├─ .gitignore        # Ignore secrets, venv, cache, raw data
└─ README.md
⚙️ Setup
Clone this repo:
git clone https://github.com/brandonjbbb/Streaming_MentalHealth_Prevalences.git
cd Streaming_MentalHealth_Prevalences
Create and activate a virtual environment:
python3 -m venv .venv
source .venv/bin/activate
Install dependencies:
pip install -r requirements.txt
Copy environment file and update values if needed:
cp .env.example .env