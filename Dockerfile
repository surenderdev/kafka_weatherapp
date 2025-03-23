FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy all local files to /app in the container
COPY . /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Command to run the Streamlit app
CMD ["streamlit", "run", "frontend.py"]
