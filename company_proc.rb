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
path_for_companies_json = '/home/danny/dev/worklocal/crunchcrawl-master/companydata.json'
# Output file
path_for_companyfile = '/home/danny/dev/worklocal/crunchcrawl-master/companies_proc.txt'

## Preloading - load the json
#========================================================
puts "Starting at #{Time.now}"
year_start = 2000
year_end = 2008
companystruct = {}
json = File.open(path_for_companies_json)
json.each_line do |line|
  company = JSON.parse(line)
  companystruct[company['permalink']] = company
end
puts "Total company struct size: #{companystruct.size}"

## Process the company struct and extract what we care about
#========================================================
newcompstruct = {}
companystruct.each do |topic,company|
  begin
    can_write = false
    # Each company has funding rounds; scan through them and
    #   keep the company if it has nonblank funding rounds with
    #   values in USD.
    company['funding_rounds'].each do |round|
      if round['raised_currency_code'] == "USD"
        can_write = true
      end
    end
    if can_write == false
      next
    else
      # We must have founding rounds, so let's summarize the the
      #   company description with key terms.
      summary = company['overview']
      text = Highscore::Content.new summary, Highscore::Blacklist.load_file('blacklist.txt')
      text.set :ignore_case, true
      newcompstruct[topic] = {}
      newcompstruct[topic][:funding] = {}
      company['funding_rounds'].each do |round|
        year = round['funded_year']
        if round['raised_currency_code'] == 'USD'
          newcompstruct[topic][:funding][year] = newcompstruct[topic][:funding][year] || 0.0
          newcompstruct[topic][:funding][year] = newcompstruct[topic][:funding][year] + round['raised_amount'] 
        end
      end

      newcompstruct[topic][:words] = {}
      text.keywords.top(NUMTOP).each do |word|
        newcompstruct[topic][:words][word.text] = word.weight
      end
    end
  rescue

  end
end

## Write out the results
#========================================================
puts "Finished processing companies.  Now writing at #{Time.now}."
File.open(path_for_companyfile,'w') do |outfile|
  JSON.dump(newcompstruct, outfile)
end

# Display to make sure everything is okay
puts "#{newcompstruct['a-bit-lucky'][:words]}"
puts "#{newcompstruct['twitter'][:words]}"
puts companystruct['twitter']['overview']
puts "Finished at #{Time.now}"
