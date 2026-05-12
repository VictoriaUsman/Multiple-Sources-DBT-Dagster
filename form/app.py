import json
import os
import sys
from flask import Flask, render_template, redirect, url_for, flash
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired, Length

# 1. IMPORT PRODUCER FIRST — fail immediately if the library is missing
try:
    from confluent_kafka import Producer
except ImportError:
    sys.exit("ERROR: 'confluent-kafka' not found. Run 'pip install confluent-kafka'")

app = Flask(__name__)

secret_key = os.environ.get('SECRET_KEY')
if not secret_key:
    raise EnvironmentError("Required environment variable 'SECRET_KEY' is not set.")
app.config['SECRET_KEY'] = secret_key

# 2. CONFIGURATION
kafka_config = {
    'bootstrap.servers': '127.0.0.1:9094', 
    'client.id': 'flask-producer',
    'acks': 'all',              
    'retries': 5,
    'delivery.timeout.ms': 10000 
}

# 3. INITIALIZE PRODUCER
producer = Producer(kafka_config)
KAFKA_TOPIC = 'host_info_topic'

def delivery_report(err, msg):
    if err is not None:
        print(f'❌ FAIL: {err}')
    else:
        # This Offset is the 'Receipt Number' from Kafka
        print(f'✅ SUCCESS: Topic {msg.topic()} | Partition [{msg.partition()}] | Offset {msg.offset()}')

# 4. FORM DEFINITION
class HostForm(FlaskForm):
    hostname = StringField('Name', validators=[DataRequired(), Length(min=2, max=100)])
    country = StringField('Country', validators=[DataRequired(), Length(min=2, max=100)])
    city = StringField('City', validators=[DataRequired(), Length(min=2, max=100)])
    contact_number = StringField('Contact Number', validators=[DataRequired(), Length(min=7, max=20)])
    submit = SubmitField('Send to Snowflake')

# 5. ROUTES
@app.route('/', methods=['GET', 'POST'])
def index():
    form = HostForm()
    if form.validate_on_submit():
        payload = {
            'hostname': form.hostname.data,
            'country': form.country.data,
            'city': form.city.data,
            'contact': form.contact_number.data
        }
        
        try:
            print(f"Pushing hostname: {payload['hostname']}...")
            producer.produce(
                KAFKA_TOPIC, 
                key=str(payload['hostname']), 
                value=json.dumps(payload), 
                callback=delivery_report
            )
            # This 'pushes' the message from your Mac into the Docker container
            producer.flush(timeout=10) 
            flash("Data sent to Kafka successfully!")
        except Exception as e:
            print(f"🔥 Error: {e}")
            flash(f"Error: {e}")

        return redirect(url_for('index'))
    return render_template('index.html', form=form)

if __name__ == '__main__':
    app.run(debug=True)