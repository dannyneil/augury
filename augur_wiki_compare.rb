require 'media_wiki'
require 'highscore'

path_for_termsgrowth = '/home/danny/Downloads/keyterms_growth_mr.txt'
mw = MediaWiki::Gateway.new('http://en.wikipedia.org/w/api.php')

# Number of summary words to generate
NUMTOP = 9
# Average results over this many years
GROUPSIZE = 1

# Choose the topics to compete
topics = ["engineering", "sports", "professor", "musician", "guidance counselor", "doctor", "author", "lawyer"]

# Year range
year_start = 2000
year_end = 2008


# Fetch a wikipedia page and follow the first redirect if it exists
def get_wiki_page mw, topic
  result = mw.get topic
  if q=result.match(/^#REDIRECT \[\[(.+)\]\]/)
    q=q[1]
    result = mw.get q
  end
  return result
end

# Generate keywords for the topics
keywords = {}
all_keywords = []
for topic in topics
  result = get_wiki_page mw, topic
  text = Highscore::Content.new result, Highscore::Blacklist.load_file('blacklist.txt')
  text.set :ignore_case, true
  keywords[topic] = text.keywords.top(NUMTOP).map {|k| "#{k}"} + [topic]
  all_keywords = (all_keywords + keywords[topic] + [topic]).uniq
end

# Tell the user what the keywords are
puts "Top #{NUMTOP+1} compare:"
for topic in topics
  puts "#{topic}: #{keywords[topic].join(", ")}"
end

# Define a scoring function
def score_term word, year_hash, year
  min_norm_count = 0.000001
  curr_nc = 0.0
  prev_nc = 0.0
  (year-GROUPSIZE+1).upto(year) do |y|
    begin
      curr_nc += year_hash[y][word][:nc] || 0.0
    rescue
      curr_nc += 0.0
    end
  end
  (year-GROUPSIZE*2+1).upto(year-GROUPSIZE) do |y|
    begin
      prev_nc += year_hash[y][word][:nc] || 0.0
    rescue
      prev_nc += 0.0
    end
  end

  if prev_nc > 0.0
    growth = curr_nc / prev_nc
  else
    growth = 0.0
  end

  if growth > 1.0
    return growth
  else
    return 1.0
  end
end

# Initialize variables
growth_by_year = {}
year_start.upto(year_end) do |year|
  growth_by_year[year] =  {}
end

# Scan these year ranges
full_year_start = year_start - GROUPSIZE*2 + 1
full_year_end = year_end
year_hash = {}
full_year_start.upto(full_year_end) do |year|
  year_hash[year] = {}
end

# Load the relevant terms
puts "Pre-analysis (data structure loading) at #{Time.now}."
infile = File.open(path_for_termsgrowth)
infile.each_line do |line|
  frags = line.split(",")
  term = frags[0].downcase
  year = Integer(frags[1])
  normcount = Float(frags[2])
  growth = Float(frags[3])

  next unless year >= full_year_start and year <= full_year_end
  year_hash[year][term] = {:nc => normcount, :g => growth}

  if (all_keywords.include? term)
    score = score_term term, year_hash, year
    if year >= year_start and year <= year_end
      growth_by_year[year][term] = score
    end
  end
end

# Build a scoring dataset
topic_scoring = {}
year_start.upto(year_end) do |year|
  topic_scoring[year] =  {}
end
topics.each do |topic|
  topic_scoring[topic] = 0.0
end
year_start.upto(year_end) do |year|
  topics.each do |topic|
    topic_sum = 0.0
    keywords[topic].each do |keyword|
      topic_sum += growth_by_year[year][keyword] || 1.0
      topic_scoring[topic] += growth_by_year[year][keyword] || 1.0
    end
    topic_scoring[year][topic] = topic_sum
  end
end
# Tell the user the year-by-year results
year_start.upto(year_end) do |year|
  puts "#{year}: #{topic_scoring[year].sort_by{|key, value| value}.map{|k,v| "#{k} (%5.5f)" % v}.reverse.join(', ')}"
end

# Tell the use the final scores
puts "Final scores"
final = {}
topics.each do |topic|
  final[topic] = topic_scoring[topic]
end

# Tell the user the complete standings, with terms
puts "Final ordering: #{final.sort_by{|k,v| v}.map{|k,v| "#{k} (%5.5f)" % v}.reverse.join(', ')}"
