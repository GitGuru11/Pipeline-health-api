# Use an official Python runtime as a parent image
FROM python:3.12.2

# Set the working directory in the container
WORKDIR /app

# Copy the requirements.txt file into the container
COPY requirements.txt .

# Install any dependencies specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Expose the port on which the Flask app will run
EXPOSE 8000

# Set environment variables
ENV FLASK_APP=app.py
ENV FLASK_ENV=production

# Ensure that the .env file is copied to the container
COPY .env .env

# Run the application
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "app:app"]
#CMD ["flask", "run", "--host=0.0.0.0"]