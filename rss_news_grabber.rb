#!/usr/bin/env ruby
## Copyright Grzegorz Blaszczyk Consulting 2011

MaxRSSItems = 100

DbName = 'foo_news'
DbTable = 'news'
RSSFeed = 'http://www.media.foo.pl/rss'

### DO NOT EDIT BELOW THIS LINE ###

require 'sqlite3'
require 'rss'

def fetch_news()
  return RSS::Parser.parse(open(RSSFeed).read, false).items[0..MaxRSSItems-1]
end

def get_news(items)
  news_array=[]
  items.each do |item|
    plain_description = item.description.gsub(/<\/?[^>]*>/, "")    

    news = Hash.new
    news['title'] = item.title
    news['guid'] = item.guid
    news['description'] = plain_description.split("...")[0]
    news['pub_date'] = item.date

    news_array << news
  end
  return news_array
end

def verify_database_sqlite
  ### Creating database if it does not exist ###
  database = SQLite3::Database.new("#{DbName}.db")
  database.execute( "CREATE TABLE IF NOT EXISTS #{DbTable} (
    id INTEGER PRIMARY KEY,
    title TEXT,
    guid TEXT,
    description TEXT,
    pub_date DATETIME
  )")
  return database
end

def save_or_update_in_database_sqlite(database, news_array)

  news_array.each do |news| 

    puts "Filling database with '#{news['title']}' -> #{news['pub_date']}..."
    select_query = "select id FROM #{DbTable} WHERE pub_date = ? ORDER BY pub_date DESC LIMIT 1"
    insert_query = "insert into #{DbTable} (id,title,guid,description,pub_date) values (null,?,?,?,?)"
    
    select_statement = database.prepare(select_query)
    insert_statement = database.prepare(insert_query)

    select_statement.bind_param(1, news['pub_date'].to_s)

    insert_statement.bind_param(1, news['title'])
    insert_statement.bind_param(2, news['guid'].to_s)
    insert_statement.bind_param(3, news['description'])
    insert_statement.bind_param(4, news['pub_date'].to_s)

    rows = select_statement.execute!(news['pub_date'].to_s)
    if !rows.nil? and rows.length > 0
      puts "Data up to date..."
    else
      puts "Executing insert for #{news['title']} ..."
      insert_statement.execute!
    end
    select_statement.close
    insert_statement.close
  end  
end

def main()
  items = fetch_news()
  database = verify_database_sqlite()
  puts "Beginning transaction..."
  database.transaction()
  save_or_update_in_database_sqlite(database, get_news(items))
  puts "Committed transaction..."
  database.commit()
end

main()

