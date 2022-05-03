#!/home/pi/pysma/bin/python3

# archive the values from midnight into a new table

from datetime import datetime, time
import sqlite3

def create_connection(db_file):
	""" create a database connection to a SQLite database """
	conn = None
	try:
		conn = sqlite3.connect(db_file)
		#print(sqlite3.version)
	except sqlite3.Error as e:
		print(e)

	return conn


def archive_db():
	conn = create_connection('/home/pi/pysma/pysma.db')
	midnight = datetime.combine(datetime.today(), time.min)

	cur = conn.cursor()

	sql = '''SELECT dateTime,total_yield, metering_total_yield, metering_total_absorbed
	FROM archive WHERE dateTime > ? order by dateTime limit 1'''

	# the (variable,) format indicates that it's a sequence, which cursor.execute(wants)
	cur.execute(sql, (midnight.timestamp(),) )

	rows = cur.fetchall()

	sql = '''INSERT INTO daily_archive(dateTime, total_yield, metering_total_yield, metering_total_absorbed)
			VALUES(?, ?, ?, ?)'''

	for row in rows:
		try:
			cur.execute(sql, row)
			conn.commit()
		except sqlite3.IntegrityError as err:
			print("Date entry already exists: ", err)
			print(row)


if __name__ == "__main__":
	archive_db()
