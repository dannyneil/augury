require 'media_wiki'
require 'highscore'
require 'json'

## Constants
#========================================================
# How many key terms to extract from company summaries:
NUMTOP = 8
# How many years to average over:
GROUPSIZE = 1
# Path for key terms with growth, produced by either the MapReduce file or ruby script
path_for_termsgrowth = '/home/danny/Downloads/keyterms_growth_mr.txt'
# JSON database for companies
path_for_companies_json = '/home/danny/dev/worklocal/crunchcrawl-master/companies_proc.txt'
# Output path
path_for_corrfile = '/home/danny/Desktop/companies_growth_dollars.txt'
# Years to calculate companies over (saves time by excluding growth in other years)
year_start = 2000
year_end = 2008
# Minimum score for growth (some are negative, and these are currently ignored and replaced with the floor score)
FLOOR = 1.0

# Load the company structure
#========================================================
puts "Starting at #{Time.now}"
companystruct = {}
File.open(path_for_companies_json) do |json|
  companystring = json.read()
  companystruct=JSON.parse(companystring)
end
# Double-check that it loaded okay
puts "#{companystruct['twitter']}"

# Build list of keywords from the JSON wordlist
all_keywords = []
companystruct.each do |comp, comphash|
  begin
    all_keywords += comphash['words'].map { |word, wt| word}
  rescue

  end
end
# Eliminate duplicates
all_keywords = all_keywords.uniq
puts "Finished building keywords at #{Time.now}"

## Build term-growth structure
#========================================================
# Scoring function - separate into one group and previous group, and 
# return the growth of new group over previous group.
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


# Create empty year-growth structures
growth_by_year = {}
year_start.upto(year_end) do |year|
  growth_by_year[year] =  {}
end
full_year_start = 1980
full_year_end = 2008
year_hash = {}
full_year_start.upto(full_year_end) do |year|
  year_hash[year] = {}
end

## Populate data structure for term growth
#========================================================
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

## Score the companies by their key terms
#========================================================
puts "Structure loaded.  Beginning scoring at #{Time.now}."
results = {}
companystruct.each do |company, comphash|
  # Only do ones that state funding
  comphash['funding'].each do |year, dollars|
    begin
      year = Integer(year) || 2008
      if year <= year_end
        growscore = 0.0
        comphash['words'].each do |word, weight|
          growscore += growth_by_year[year][word] || FLOOR
        end
        results["#{company}-#{year}"] = {:grow => growscore, :dollars => dollars}
      end
    rescue
      puts "Error with #{company} on #{year} with #{comphash}"
    end
  end
end

## Write results out - each company-year, their growth score, and their funding dollars
#========================================================
File.open(path_for_corrfile,'w') do |outfile|
  results.each do |term, scores|
    outfile.syswrite("#{term},#{scores[:grow]},#{scores[:dollars]}\n")
  end
end
puts "Finished at #{Time.now}"
