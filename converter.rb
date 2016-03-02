require 'mongo'
require 'csv'

client = Mongo::Client.new(ENV['mongodb_uri'], :max_pool_size => 10)

gems = client[:gems]
dataset = []
dataset << ['name','average.downloads', 'download.pattern', 'weekday.downloads.percentage',
            'average.commits.per.day', 'weekday.commits.percentage', 'commit.pattern',
            'average.issue.resolution.time', 'issue.pattern', 'top.half.contributors.contribution',
            'average.commits.per.contributor', 'contributors',
            'average.forks', 'average.stars', 'last.commit.days']

gems.find().each do |document|
  # Average downloads
  first_publish = document['version_downloads_days'].first['created_at']
  duration = (Date.today - Date.parse(first_publish)).to_i
  average_downloads = document['total_downloads'].to_f / duration.to_f

  # Donwloads pattern
  downloads_aggregation = Hash.new(0)
  document['version_downloads_days'].each do |version|
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

  # Percentage of downloads on week day
  total_downloads = downloads_aggregation.values.reduce(:+)
  weekend_downloads = 0
  downloads_aggregation.each_pair do |date, downloads|
    weekend_downloads += downloads if Date.parse(date).sunday? || Date.parse(date).saturday?
  end
  weekday_downloads_percent =  1 - (weekend_downloads.to_f / total_downloads.to_f)

  # Average commits (per day)
  if !document['commits'] || document['commit_history'].nil? || document['commit_history'].length == 0
    average_commits = ''
  else
    duration = (Date.parse(document['created_at'].to_s) - Date.parse(document['commit_history'].first['created_at'])).to_i
    average_commits = document['commits'].to_f / duration.to_f
  end

  # Percentage of commits on week days
  if document['commit_history'].nil? || document['commit_history'].length == 0
    weekday_commit_percent = ''
  else
    weekday_commits = 0
    document['commit_history'].each do |commit|
      weekday_commits += 1 if !Date.parse(commit['created_at']).sunday? && !Date.parse(commit['created_at']).saturday?
    end
    weekday_commit_percent = weekday_commits.to_f / document['commit_history'].length.to_f
  end

  # Commit pattern
  if !document['commit_history']
    commit_pattern = ''
  else
    duration = (Date.parse(document['created_at'].to_s) - Date.parse(document['commit_history'].first['created_at'])).to_i
    mid_date = Date.parse(document['commit_history'].first['created_at']) + (duration.to_f / 2).round
    first_half_commits, second_half_commits = 0, 0
    document['commit_history'].each do |commit|
      Date.parse(commit['created_at']) >= mid_date ? second_half_commits += 1 : first_half_commits += 1
    end
    commit_pattern = second_half_commits.to_f / first_half_commits
  end

  # Average issue resolution time
  if document['issues_info'].nil? || document['issues_info'].length == 0
    average_issue_resolution_time = ''
  else
    total_duration = 0
    document['issues_info'].each do |issue|
      total_duration += issue['duration']
    end
    average_issue_resolution_time = total_duration.to_f / document['issues_info'].length.to_f
  end

  # Close issues pattern
  if document['commit_history'].nil? || document['commit_history'].length == 0 || document['issues_info'].nil? || document['issues_info'].length == 0
    issue_pattern = ''
  else
    duration = (Date.parse(document['created_at'].to_s) - Date.parse(document['commit_history'].first['created_at'])).to_i
    mid_date = Date.parse(document['commit_history'].first['created_at']) + (duration.to_f / 2).round
    first_half_issues, second_half_issues = 0, 0
    document['issues_info'].each do |issue|
      Date.parse(issue['created_at']) >= mid_date ? second_half_issues += 1 : first_half_issues += 1
    end
    issue_pattern = second_half_issues.to_f / first_half_issues.to_f
  end

  # Percentage of the top half contributors commits
  if document['contributors'].nil? || document['contributors'].length == 0 || !document['commits']
    percentage_top_half_contributors_commits = ''
  else
    half_contributors = (document['contributors'].length.to_f / 2).round
    half_contributors_total = 0
    document['contributors'][0..half_contributors].each do |contributor|
      half_contributors_total += contributor['contributions']
    end
    percentage_top_half_contributors_commits = half_contributors_total.to_f / document['commits'].to_f
  end

  # Average commits (per contributors)
  if !document['commits'] || document['contributors'].nil? || document['contributors'].length == 0
    average_contributor_commits = ''
  else
    average_contributor_commits = document['commits'].to_f / document['contributors'].length.to_f
  end

  # Number of contributors
  if document['contributors'].nil? || document['contributors'].length == 0
    contributors_number = ''
  else
    contributors_number = document['contributors'].length
  end

  # Average forks (per day)
  if document['commit_history'].nil? || document['commit_history'].length == 0 || !document['forks']
    average_forks = ''
  else
    duration = (Date.parse(document['created_at'].to_s) - Date.parse(document['commit_history'].first['created_at'])).to_i
    average_forks = document['forks'].to_f / duration.to_f
  end

  # Average stars (per day)
  if document['commit_history'].nil? || document['commit_history'].length == 0 || !document['stars']
    average_stars = ''
  else
    duration = (Date.parse(document['created_at'].to_s) - Date.parse(document['commit_history'].first['created_at'])).to_i
    average_stars = document['stars'].to_f / duration.to_f
  end
  
  # Last commit day
  last_commit_days = document['last_commit']

  dataset << [ 
    document['name'], average_downloads, downloads_pattern,
    weekday_downloads_percent, average_commits,
    weekday_commit_percent, commit_pattern,
    average_issue_resolution_time, issue_pattern,
    percentage_top_half_contributors_commits,
    average_contributor_commits, contributors_number,
    average_forks, average_stars, last_commit_days
  ]
  puts document['name']
end

CSV.open('data.csv', 'w') do |csv|
    dataset.each do |row|
      csv << row
    end
end