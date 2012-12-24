require 'redis'
require 'hiredis'
require 'zlib'
require 'rbtree'

path_for_gzips = '/home/danny/dev/worklocal/'
path_for_norms = '/home/danny/Dropbox/project/augur/googlebooks-eng-all-totalcounts-20090715.txt'
path_for_summary_out = '/home/danny/Dropbox/project/augur/keyterms.txt'
path_for_full_out = '/home/danny/dev/worklocal/keyterms_growth_rb.txt'

redis = Redis.new(:driver => :hiredis)
redis.flushdb

# Constants
min_norm_count = 0.000001
groupsize = 5
top_x = 15
year_start = 1890
year_end = 2008

## Load normalization constants
#========================================================
puts "Pre-analysis (norm loading) at #{Time.now}."

=begin
total_vols = {}
total_counts = {}
year_start.upto(year_end).each do |year|
  total_counts[year] = 0
end

infile = open(path_for_norms)
Dir.entries(path_for_gzips).each do |f|
  if f.split('.')[-1] == 'gz'
    infile = open(path_for_gzips+f)
    gz_in = Zlib::GzipReader.new(infile)
    gz_in.each_line do |line|
      frags = line.split("\t")
      term = frags[0].downcase
      year = Integer(frags[1])
      count = Integer(frags[2])
      next unless year >= year_start
      # next unless normcount > min_norm_count
      next unless /^[A-Za-z+'-]+$/.match(term)
      total_counts[year] += count
    end
    puts "Finished '#{f}', total_counts: #{total_counts}."
  end
end
=end
# Already calculated and saved here!  You can uncomment the above if you'd prefer to recalculate
total_counts = {1890=>637223145, 1891=>678202886, 1892=>761884866, 1893=>711740472, 1894=>706979365, 1895=>761508003, 1896=>813354925, 1897=>799997477, 1898=>850736620, 1899=>964942480, 1900=>1006474587, 1901=>1012030615, 1902=>1019621189, 1903=>1010112550, 1904=>1108600155, 1905=>1089517889, 1906=>1079629634, 1907=>1105531073, 1908=>1104399225, 1909=>977566731, 1910=>1006992383, 1911=>1019393521, 1912=>1063222145, 1913=>1022089584, 1914=>1008549308, 1915=>910596976, 1916=>903020151, 1917=>905809289, 1918=>799229158, 1919=>840238950, 1920=>1058489870, 1921=>917266159, 1922=>1069099819, 1923=>842867524, 1924=>765188484, 1925=>780592957, 1926=>785228225, 1927=>872230847, 1928=>853824887, 1929=>826162456, 1930=>884031270, 1931=>852526450, 1932=>753968985, 1933=>674958118, 1934=>717716934, 1935=>815889423, 1936=>834632136, 1937=>855033443, 1938=>859601393, 1939=>858595577, 1940=>809040402, 1941=>757342683, 1942=>757260325, 1943=>660792452, 1944=>596657882, 1945=>675389183, 1946=>889057981, 1947=>1064710459, 1948=>1102886302, 1949=>1201288200, 1950=>1242448246, 1951=>1189915284, 1952=>1237619618, 1953=>1190916766, 1954=>1227172662, 1955=>1293591462, 1956=>1346642547, 1957=>1431460328, 1958=>1486537296, 1959=>1522410674, 1960=>1802783819, 1961=>1919152736, 1962=>2081896675, 1963=>2189394415, 1964=>2038951146, 1965=>2353897383, 1966=>2423568231, 1967=>2583196029, 1968=>2772547998, 1969=>2601530305, 1970=>2689572797, 1971=>2534937013, 1972=>2578939710, 1973=>2501455748, 1974=>2429956918, 1975=>2484011803, 1976=>2557740994, 1977=>2569035190, 1978=>2562834933, 1979=>2677287907, 1980=>2669702338, 1981=>2566349098, 1982=>2690358389, 1983=>2796099372, 1984=>3005816304, 1985=>3012598644, 1986=>3149573268, 1987=>3198195133, 1988=>3331594776, 1989=>3431629821, 1990=>3719824957, 1991=>3571089906, 1992=>3844401461, 1993=>3862863740, 1994=>4155348313, 1995=>4404326130, 1996=>4826935124, 1997=>4946176107, 1998=>5298290572, 1999=>5613521772, 2000=>6683408567, 2001=>7013148488, 2002=>7919926694, 2003=>8970289787, 2004=>9800620399, 2005=>9295461177, 2006=>9731037439, 2007=>10336339389, 2008=>11307408434}

## Populate redis with normalized terms
puts "Launching analysis at #{Time.now}."
buf = {}
last_term = ""
years_to_write = []
write_term = false
Dir.entries(path_for_gzips).each do |f|
  if f.split('.')[-1] == 'gz'
    infile = open(path_for_gzips+f)
    gz_in = Zlib::GzipReader.new(infile)
    gz_in.each_line do |line|
      frags = line.split("\t")
      term = frags[0].downcase
      year = Integer(frags[1])
      count = Float(frags[2])
      if frags.size != 5
        puts frags
        throw "Too many frags (#{frags.size}) in main datafiles!"
      end
      if term != last_term
        if years_to_write.size > 0 and write_term
          years_to_write.each do |k|   
            redis.incrbyfloat k, buf[k]
          end
        end
        # Reset
        write_term = false
        years_to_write = []
        last_term = term
        buf = {}
      end
      next unless year >= year_start
      normcount = count / Float(total_counts[year])
      next unless /^[A-Za-z+'-]+$/.match(term)
      cnt_key = "normcnt:grp:#{year/groupsize}:#{term}"
      buf[cnt_key] = normcount
      if normcount > min_norm_count
        write_term = true 
        years_to_write << cnt_key
        years_to_write << "normcnt:grp:#{year/groupsize - 1}:#{term}"
        redis.sadd "allwords", "#{last_term}:#{year}"
      end
    end
    puts "Finished '#{f}', allwords size: #{redis.scard "allwords"}."
  end
end
if years_to_write.size > 0 and write_term
  years_to_write.each do |k|   
    redis.incrbyfloat k, buf[k]
  end
end
puts "Finished populating redis with terms at #{Time.now}."
begin
  redis.save
rescue
  puts "Problem with saving the redis db."
end

## Find growth terms
#========================================================
puts "Launching growth finder at #{Time.now}."

# Initialize hash with fast red-black trees
year_hash = {}
(year_start..year_end).each do |year|
  year_hash[year] = RBTree.new
end
# Create a working copy (memory intensive, but safer!)
redis.sunionstore "tempwords", "allwords"
File.open(path_for_full_out,"w") do |f|
  while word = redis.spop("tempwords") do
    tokens = word.split(":")
    term = tokens[0]
    year = Integer(tokens[1])
      
    # Get data
    year_key = "normcnt:grp:#{year/groupsize}:#{term}"
    prev_year_key = "normcnt:grp:#{year/groupsize - 1}:#{term}"
    normcount = Float(redis.get(year_key) || 0.0)
    prev_norm_count = Float(redis.get(prev_year_key) || 0.0)

    # Calculate growth
    if prev_norm_count > 0.0
      growth = normcount / prev_norm_count / groupsize
    else
      growth = 0.0
    end
    
    # Output it
    f.syswrite("#{term},#{year},#{normcount},#{growth}\n")

    # See if it makes the cut
    min_pair = year_hash[year].first || [0.0, ""]
    if growth > 0.0 and (growth > min_pair[0] or year_hash[year].size < top_x)
      year_hash[year][growth] = term
      year_hash[year].delete(year_hash[year].first[0]) if year_hash[year].size >= top_x
    end
  end
end

## Write summary
#========================================================
File.open(path_for_summary_out,"w") do |f|
  f.syswrite("Augury Summary, generated #{Time.now}\n")
  f.syswrite("Results calculated in #{groupsize} year chunks.\n")
  (year_start..year_end).each do |year|
    yearstring = "#{year}: #{year_hash[year].map{|k,v| "#{v} (%1.1f)" % k}.reverse.join(', ')}"
    puts yearstring
    f.syswrite("#{yearstring}\n")
  end
end

puts "Finished analysis at #{Time.now}."
