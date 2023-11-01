import pyodbc
import datetime
import smtplib

# Replace the following placeholder values with your own values:
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
EMAIL_SENDER = 'sender@gmail.com'
EMAIL_RECIPIENT = 'recipient@gmail.com'
CPU_UTILIZATION_THRESHOLD = 90
MEMORY_USAGE_THRESHOLD = 80
IO_WAIT_TIME_THRESHOLD = 10

class BSESYottaMumbaiServerMonitor:
    def __init__(self, oracle_database_server):
        self.oracle_database_server = oracle_database_server

        # Create a pyodbc connection to the Oracle database server
        self.oracle_database_connection = pyodbc.connect('DRIVER={Oracle};SERVER={oracle_database_server}'.format(oracle_database_server=self.oracle_database_server))

    def monitor(self):
        # Get the statistics from the Oracle database server
        stats = self.get_stats()

        # Generate alerts if any of the KPIs exceed the set thresholds
        if stats["average_cpu_utilization"] > CPU_UTILIZATION_THRESHOLD:
            self.send_email_alert('CPU utilization is high on BSES YOTTA MUMBAI SERVER', 'CPU utilization is at {}%'.format(stats["average_cpu_utilization"]))

        if stats["average_memory_usage"] > MEMORY_USAGE_THRESHOLD:
            self.send_email_alert('Memory usage is high on BSES YOTTA MUMBAI SERVER', 'Memory usage is at {}%'.format(stats["average_memory_usage"]))

        if stats["average_io_wait_time"] > IO_WAIT_TIME_THRESHOLD:
            self.send_email_alert('I/O wait time is high on BSES YOTTA MUMBAI SERVER', 'I/O wait time is {}ms'.format(stats["average_io_wait_time"]))

    def send_email_alert(self, subject, body):
        # Send an email alert using your preferred email provider
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as smtp_server:
            smtp_server.starttls()
            smtp_server.login(EMAIL_SENDER, EMAIL_RECIPIENT)
            smtp_server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENT, 'Subject: {}\n\n{}'.format(subject, body))

    def get_stats(self):
        """Gets the CPU utilization, memory usage, and I/O wait time statistics from the Oracle database server.

        Returns:
            A dictionary containing the following keys:
                * cpu_utilization: A list of tuples, where each tuple contains the CPU usage and the number of samples for that CPU usage.
                * memory_usage: A list of tuples, where each tuple contains the memory usage and the number of samples for that memory usage.
                * io_wait_time: A list of tuples, where each tuple contains the I/O wait time and the number of samples for that I/O wait time.
        """

        # Get the CPU utilization statistics from the Oracle database server
        cpu_utilization_stats = self.oracle_database_connection.cursor().execute(
            """
            SELECT CPU_USAGE, COUNT(*) AS num_samples
            FROM V$SYS_TIME_MACHINE
            GROUP BY CPU_USAGE
            """
        ).fetchall()

        # Get the memory usage statistics from the Oracle database server
        memory_usage_stats = self.oracle_database_connection.cursor().execute(
            """
            SELECT SGA_ALLOC, COUNT(*) AS num_samples
            FROM V$SGASTAT
            GROUP BY SGA_ALLOC
            """
        ).fetchall()

        # Get the I/O wait time statistics from the Oracle database server
        io_wait_time_stats = self.oracle_database_connection.cursor().execute(
            """
                SELECT TIME_WAITED, COUNT(*) AS num_samples
        FROM V$SESSION_WAIT
        GROUP BY TIME_WAITED
        """
        ).fetchall()

        # Calculate the average CPU utilization, memory usage, and I/O wait time
        average_cpu_utilization = sum(cpu_utilization_stats[i][0] * cpu_utilization_stats[i][1] for i in range(len(cpu_utilization_stats))) / sum(cpu_utilization_stats[i][1] for i in range(len(cpu_utilization_stats)))
        average_memory_usage = sum(memory_usage_stats[i][0] * memory_usage_stats[i][1] for i in range(len(memory_usage_stats))) / sum(memory_usage_stats[i][1] for i in range(len(memory_usage_stats)))
        average_io_wait_time = sum(io_wait_time_stats[i][0] * io_wait_time_stats[i][1] for i in range(len(io_wait_time_stats))) / sum(io_wait_time_stats[i][1] for i in range(len(io_wait_time_stats)))

        # Return the statistics
        return {
            "cpu_utilization": cpu_utilization_stats,
            "memory_usage": memory_usage_stats,
            "io_wait_time": io_wait_time_stats,
            "average_cpu_utilization": average_cpu_utilization,
            "average_memory_usage": average_memory_usage,
            "average_io_wait_time": average_io_wait_time
        }

if __name__ == '__main__':
    oracle_database_server = '192.168.1.100'
    monitor = BSESYottaMumbaiServerMonitor(oracle_database_server)

    # Monitor the Oracle database server
    monitor.monitor()
