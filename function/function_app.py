import logging
import azure.functions as func
import psycopg2
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="ServiceBusQueueTrigger", connection="sangtt4sb_SERVICEBUS") 
def ServiceBusQueueTrigger(azservicebus: func.ServiceBusMessage):
    notification_id = int(azservicebus.get_body().decode('utf-8'))
    logging.info('Python ServiceBus Queue trigger processed a message: %s',
                azservicebus.get_body().decode('utf-8'))

# TODO: Get connection to database
    connection = psycopg2.connect(
        host=os.environ["DB_HOST"],
        database=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PWD"]
    )
    cursor = connection.cursor()
    try:
        # TODO: Get notification message and subject from database using the notification_id
        cursor.execute("SELECT subject, message FROM notification WHERE id=(%s);", ([notification_id]))
        subject, message = cursor.fetchone()


        # TODO: Get attendees email and name
        cursor.execute("SELECT first_name, email FROM attendee;")
        attendees = cursor.fetchall()

        # TODO: Loop through each attendee and send an email with a personalized subject
        for attendee in attendees: 
            logging.info("Send message with subject: " + subject +". Message: " + message + " to " + attendee[0] + " with email " + attendee[1])
            email = Mail(
                from_email='tuansang9710@gmail.com',
                to_emails=attendee[1],
                subject=subject,
                html_content=message)
            try:
                sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
                response = sg.send(email)
                logging.info(response.status_code)
                logging.info(response.body)
                logging.info(response.headers)
            except Exception as e:
                logging.error(e.message)
                
        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        updated_status = "Notified " + str(len(attendees)) + " attendees"
        cursor.execute("UPDATE notification SET status=(%s), completed_date=NOW() WHERE id=(%s)", (updated_status, notification_id))
        connection.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # TODO: Close connection
        cursor.close()
        connection.close