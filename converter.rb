require 'mongo'
require 'csv'
require 'base64'

client = Mongo::Client.new(ENV['mongodb_uri'], :max_pool_size => 10)

gems = client[:gems]
dataset = []
dataset << ['name','average.downloads', 'download.pattern', 'weekday.downloads.percentage',
            'average.commits.per.day', 'weekday.commits.percentage', 'commit.pattern',
            'average.issue.resolution.time', 'issue.pattern', 'top.contributors.contribution',
            'average.commits.per.contributor', 'contributors',
            'average.forks', 'average.stars', 'last.commit.days', 'readme.word.count']

# Average downloads per day
def get_average_downloads(data)
  if data['version_downloads_days'].nil? || data['version_downloads_days'].empty?
    average_downloads = 0
  else
    first_publish = data['version_downloads_days'].first['created_at']
    duration = (Date.today - Date.parse(first_publish)).to_i
    average_downloads = data['total_downloads'].to_f / duration.to_f
  end

  average_downloads
end

# Second half downloads / First half downloads
def get_download_pattern(data)
  if data['version_downloads_days'].nil? || data['version_downloads_days'].empty?
    downloads_pattern = 0
  else
    downloads_aggregation = Hash.new(0)
    data['version_downloads_days'].each do |version|
      version['downloads_date'].each_pair do |key, downloads|
        downloads_aggregation[key] += downloads
      end
    end
    duration = downloads_aggregation.length
    mid_date = Date.parse(downloads_aggregation.keys[(duration / 2).round - 1])
    first_half_downloads, second_half_downloads = 0, 0
    downloads_aggregation.each_pair do |date, downloads|
      Date.parse(date) >= mid_date ? second_half_downloads += downloads : first_half_downloads += downloads
    end
    downloads_pattern = second_half_downloads.to_f / first_half_downloads.to_f
  end

  downloads_pattern
end

# Percentage of downloads on weekday
def get_percentage_downloads_weekday(data) 
  if data['version_downloads_days'].nil? || data['version_downloads_days'].empty?
    weekday_downloads_percent = 0
  else
    downloads_aggregation = Hash.new(0)
    data['version_downloads_days'].each do |version|
      version['downloads_date'].each_pair do |key, downloads|
        downloads_aggregation[key] += downloads
      end
    end
    total_downloads = downloads_aggregation.values.reduce(:+)
    weekend_downloads = 0
    downloads_aggregation.each_pair do |date, downloads|
      weekend_downloads += downloads if Date.parse(date).sunday? || Date.parse(date).saturday?
    end
    weekday_downloads_percent =  1 - (weekend_downloads.to_f / total_downloads.to_f)
  end
  
  weekday_downloads_percent
end

# Average commits per day
def get_average_commits_days(data)
  if !data['commits'] || data['commit_history'].nil? || data['commit_history'].length == 0
    average_commits = ''
  else
    duration = (Date.parse(data['created_at'].to_s) - Date.parse(data['commit_history'].first['created_at'])).to_i
    average_commits = data['commits'].to_f / duration.to_f
  end

  average_commits
end

# Percentage of commits on weekday
def get_percentage_commits_weekday(data)
  if data['commit_history'].nil? || data['commit_history'].length == 0
    weekday_commit_percent = ''
  else
    weekday_commits = 0
    data['commit_history'].each do |commit|
      weekday_commits += 1 if !Date.parse(commit['created_at']).sunday? && !Date.parse(commit['created_at']).saturday?
    end
    weekday_commit_percent = weekday_commits.to_f / data['commit_history'].length.to_f
  end

  weekday_commit_percent
end

# Second half commits / First half commits
def get_commit_pattern(data)
  if !data['commit_history']
    commit_pattern = ''
  else
    duration = (Date.parse(data['created_at'].to_s) - Date.parse(data['commit_history'].first['created_at'])).to_i
    mid_date = Date.parse(data['commit_history'].first['created_at']) + (duration.to_f / 2).round
    first_half_commits, second_half_commits = 0, 0
    data['commit_history'].each do |commit|
      Date.parse(commit['created_at']) >= mid_date ? second_half_commits += 1 : first_half_commits += 1
    end
    commit_pattern = second_half_commits.to_f / first_half_commits
  end

  commit_pattern
end

# Average time of issue resolution
def get_average_issue_resolution(data)
  if data['issues_info'].nil? || data['issues_info'].length == 0
    average_issue_resolution_time = ''
  else
    total_duration = 0
    data['issues_info'].each do |issue|
      total_duration += issue['duration']
    end
    average_issue_resolution_time = total_duration.to_f / data['issues_info'].length.to_f
  end

  average_issue_resolution_time
end

# Second half closed issues / First half closed issues
def get_closed_issue_pattern(data)
  if data['commit_history'].nil? || data['commit_history'].length == 0 || data['issues_info'].nil? || data['issues_info'].length == 0
    issue_pattern = ''
  else
    duration = (Date.parse(data['created_at'].to_s) - Date.parse(data['commit_history'].first['created_at'])).to_i
    mid_date = Date.parse(data['commit_history'].first['created_at']) + (duration.to_f / 2).round
    first_half_issues, second_half_issues = 0, 0
    data['issues_info'].each do |issue|
      Date.parse(issue['created_at']) >= mid_date ? second_half_issues += 1 : first_half_issues += 1
    end
    issue_pattern = second_half_issues.to_f / first_half_issues.to_f
  end

  issue_pattern
end

# Percentage of top two contributors' commits
def get_percentage_top_contributor_commits(data)
  if data['contributors'].nil? || data['contributors'].length == 0 || !data['commits']
    percentage_top_contributors_commits = ''
  else
    if data['contributors'].length > 1
      top_contributors_total = 0
      data['contributors'][0..1].each do |contributor|
        top_contributors_total += contributor['contributions']
      end
      percentage_top_contributors_commits = top_contributors_total.to_f / data['commits'].to_f
    else
      percentage_top_contributors_commits = 1
    end
  end

  percentage_top_contributors_commits
end

# Average commits per contributor
def get_average_commits_contributors(data)
  if !data['commits'] || data['contributors'].nil? || data['contributors'].length == 0
    average_contributor_commits = ''
  else
    average_contributor_commits = data['commits'].to_f / data['contributors'].length.to_f
  end
  average_contributor_commits
end

# Number of contributors
def get_number_contributors(data)
  if data['contributors'].nil? || data['contributors'].length == 0
    contributors_number = ''
  else
    contributors_number = data['contributors'].length
  end

  contributors_number
end

# Average stars per day
def get_average_stars(data)
  if data['commit_history'].nil? || data['commit_history'].length == 0 || !data['stars']
    average_stars = ''
  else
    duration = (Date.parse(data['created_at'].to_s) - Date.parse(data['commit_history'].first['created_at'])).to_i
    average_stars = data['stars'].to_f / duration.to_f
  end

  average_stars
end

# Average forks per day
def get_average_forks(data)
  if data['commit_history'].nil? || data['commit_history'].length == 0 || !data['forks']
    average_forks = ''
  else
    duration = (Date.parse(data['created_at'].to_s) - Date.parse(data['commit_history'].first['created_at'])).to_i
    average_forks = data['forks'].to_f / duration.to_f
  end

  average_forks
end

# Days since last commit
def get_last_commit_days(data)
  if data['last_commit'].nil?
    last_commit_days = ''
  else
    last_commit_days = data['last_commit']
  end

  last_commit_days
end

# Total readme word count
def get_total_readme_word_count(data)
  total_word_count = 0
  if data['readme_raw_text'].nil?
    total_word_count = 0
  else
    readme_text = Base64.decode64(data['readme_raw_text']['content'])
    readme_text = readme_text.gsub(/[\r\n]/, ' ')
    words = readme_text.split(' ')
    words.each do |word|
      if word =~ /^\w+$/
        total_word_count += 1
      end
    end
  end

  total_word_count
end

gems.find().each do |document|
  average_downloads = get_average_downloads(document)
  downloads_pattern = get_download_pattern(document)
  weekday_downloads_percent = get_percentage_downloads_weekday(document)
  average_commits = get_average_commits_days(document)
  weekday_commit_percent = get_percentage_commits_weekday(document)
  commit_pattern = get_commit_pattern(document)
  average_issue_resolution_time = get_average_issue_resolution(document)
  issue_pattern = get_closed_issue_pattern(document)
  percentage_top_contributors_commits = get_percentage_top_contributor_commits(document)
  average_contributor_commits = get_average_commits_contributors(document)
  contributors_number = get_number_contributors(document)
  average_forks = get_average_forks(document)
  average_stars = get_average_stars(document)
  last_commit_days = get_last_commit_days(document)
  total_word_count = get_total_readme_word_count(document)

  dataset << [ 
    document['name'], average_downloads, downloads_pattern,
    weekday_downloads_percent, average_commits,
    weekday_commit_percent, commit_pattern,
    average_issue_resolution_time, issue_pattern,
    percentage_top_contributors_commits,
    average_contributor_commits, contributors_number,
    average_forks, average_stars, last_commit_days, total_word_count
  ]
  puts document['name']
end

CSV.open('data-100-test.csv', 'w') do |csv|
    dataset.each do |row|
      csv << row
    end
end