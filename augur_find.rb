require 'zlib'
require 'rbtree'

# Source for growth terms
path_for_termsgrowth = '/home/danny/Downloads/keyterms_growth_mr.txt'
# Average over GROUPSIZE years
GROUPSIZE = 20
path_for_summary_out = "/home/danny/Dropbox/project/augur/keyterms_scored_#{GROUPSIZE}.txt"

# Set data start and end
year_start = 1890
year_end = 2008

# Initialize variables
year_hash = {}
top_hash = {}
top_x = 15
year_start.upto(year_end) do |year|
  year_hash[year] = {}
  top_hash[year] = RBTree.new
end

# Define the scoring function
# This can be way optimized in the future with dynamic programming
# (there is lots of recomputed computation).
def score_term word, year_hash, year
  # Must exceed this usage floor
  min_norm_count = 0.000001
  curr_nc = 0.0
  prev_nc = 0.0
  # Get total normalized usage counts over current period
  (year-GROUPSIZE+1).upto(year) do |y|
    begin
      curr_nc += year_hash[y][word][:nc] || 0.0
    rescue
      curr_nc += 0.0
    end
  end
  # Get total normalized usage counts over previous period
  (year-GROUPSIZE*2+1).upto(year-GROUPSIZE) do |y|
    begin
      prev_nc += year_hash[y][word][:nc] || 0.0
    rescue
      prev_nc += 0.0
    end
  end

  # Check to prevent divide  y zero
  if prev_nc > 0.0
    growth = curr_nc / prev_nc
  else
    growth = 0.0
  end

  # Can balance growth with normalized usage; currently
  #  only using growth.
  return growth #+ Math.log(nc*1000000)
end

# Load all the terms into a data structure in memory
#   and calculate winning scores at the same time.
puts "Pre-analysis (data structure loading) at #{Time.now}."
infile = File.open(path_for_termsgrowth)
infile.each_line do |line|
  frags = line.split(",")
  term = frags[0].downcase
  year = Integer(frags[1])
  normcount = Float(frags[2])
  growth = Float(frags[3])
  year_hash[year][term] = {:nc => normcount, :g => growth}

  score = score_term term, year_hash, year
  min_pair = top_hash[year].first || [0.0, ""]
  if score > 0.0 and (score > min_pair[0] or top_hash[year].size < top_x)
    top_hash[year][score] = term
    top_hash[year].delete(top_hash[year].first[0]) if top_hash[year].size >= top_x
  end
end

# Output results
File.open(path_for_summary_out,"w") do |f|
  (year_start..year_end).each do |year|
    yearstring = "#{year}: #{top_hash[year].map{|k,v| "#{v} (%1.1f)" % k}.reverse.join(', ')}"
    puts yearstring
    f.syswrite("#{yearstring}\n")
  end
end
puts "Finished ask at #{Time.now}."
