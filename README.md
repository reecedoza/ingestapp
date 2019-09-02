# Ingest App
Ingest application takes CSV files and creates a SQLite database table record of that CSV file.

## Usage
1. Clone repository
2. Import dependencies (JDBC)(Maven)
3. Build project
4. Place ms3Interview.csv file in the project root dir
5. Run application

## Approach
For this application I knew that the longest part would be either the parsing or the insertions to the database so that is what I mainly focused on. I used a prepared statement, as my inital attempt with regular statements was slow and prone to errors (as well as sql injections), that way I was able to create batches of insertion queries without having to execute them immediately. Also using prepared statements allowed me to directly insert the data as I was parsing through it.

The delay of execution also posed another problem. With so many insertion queries in the batch my applciation would take too long to execute at the end of the app so instead I added a check in the parse so that when 1000 iterations of insertions batches are added execute those. That way we don't wait till the end to execute everything.

As for the logging, I created 3 variables to keep track of the records that we were receiving as well as the ones that failed or succeded. Then at the end of the application I pass these variables to a separate funtion which deals specifically with logging.
