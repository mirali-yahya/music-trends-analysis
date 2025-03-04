# Install and load necessary libraries
install.packages("dplyr")
library(dplyr)
library(gt)
library(arrow)
library(ggplot2)

### Weekly Top 200 2024
top200 <- read_parquet("/Users/sophiepoll/Desktop/MSc Marketing/Masterarbeit/Daten/weekly_top_200_2024.parquet")
# top200 <- read_parquet(file.choose())  # You can remove this if you're already specifying the file path

# Check the structure of the 'top200' data
head(top200)
str(top200)

### Songs Meta 2024
meta <- read_parquet("/Users/sophiepoll/Desktop/MSc Marketing/Masterarbeit/Daten/song_meta_2024.parquet")
# meta <- read_parquet(file.choose())  # Same as above, no need for file.choose() if you have a path

# Check the structure of the 'meta' data
head(meta)
str(meta)

# Check column names to ensure correct joining
names(top200)
names(meta)

# Ensure the 'week_date' column is in Date format (from 'top200' dataset)
top200$week_date <- as.Date(top200$week_date)

# Ensure the 'release_date' column is in Date format (from 'meta' dataset)
meta$release_date <- as.Date(meta$release_date)

# Check if the conversion was successful
str(top200$week_date)
str(meta$release_date)

# Full join the datasets by 'track_id'
bigset <- full_join(top200, meta, by = "track_id")

# Check the first few rows to ensure the merge was successful
head(bigset)


##############################################################################
##################           Looking at the Data           ###################     
##############################################################################

# List the unique countries in the 'country' column
unique_countries <- unique(bigset$country)

# View the list of unique countries
unique_countries


##############################################################################
#############Biggest markets
##############################################################################

library(dplyr)
library(ggplot2)


# Calculate total streams per country
country_streams <- bigset%>%
  group_by(country) %>%
  summarise(total_streams = sum(streams, na.rm = TRUE)) %>%
  arrange(desc(total_streams))

# Display the ranked list of countries based on total streams
ranked_countries <- country_streams

# Print the ranked list
print(ranked_countries)

# Visualization of the top 11 countries with most streams
top11_countries <- country_streams %>%
  top_n(11, total_streams)
print(top11_countries)

ggplot(top11_countries, aes(x = reorder(country, total_streams), y = total_streams / 1e6, fill = ifelse(total_streams == max(total_streams), "red", "grey"))) +
  geom_bar(stat = "identity") +
  scale_fill_manual(values = c("red" = "red", "grey" = "grey")) +  # Define colors
  scale_y_continuous(breaks = seq(0, max(top11_countries$total_streams) / 1e6, by = 10000)) +  # Add ticks every 10M
  labs(title = "Top 10 Countries (incl. Global) by Total Streams",
       x = "Country",
       y = "Total Streams (in Millions)") +
  theme_minimal() +
  theme(legend.position = "none")  # Remove legend

##############################################################################
#############Biggest songs each country
##############################################################################

# Find the most streamed songs in the top 11 countries
top11_country_songs <- bigset %>%
  filter(country %in% top12_countries$country) %>%
  group_by(country, track_name) %>%
  summarise(total_streams = sum(streams, na.rm = TRUE)) %>%
  arrange(country, desc(total_streams)) %>%
  group_by(country) %>%
  slice(1) %>%
  ungroup()

# Print the list of most streamed songs in the top 11 countries
print(top11_country_songs)



### Visual fpr most streamed songs
library(ggplot2)

ggplot(top11_country_songs, aes(x = reorder(country, total_streams), y = total_streams / 1e6, fill = track_name)) +
  geom_bar(stat = "identity") +
  geom_text(aes(label = round(total_streams / 1e6, 1)),  # Add labels showing total streams
            vjust = -0.5,  # Position text above bars
            size = 4) +  # Adjust text size
  scale_y_continuous(breaks = seq(0, max(top11_country_songs$total_streams) / 1e6, by = 100)) +  # Set y-axis scale every 100M
  labs(title = "Most Streamed Songs in Top 10 Countries and Global",
       y = "Total Streams (in Millions)",  
       x = "Country",  
       fill = "Track Name") +
  theme_minimal() +
  theme(axis.text.x = element_text(size = 12),  
        axis.text.y = element_text(size = 12))  

##############################################################################
### Create a list which includes songs which have been rank 1st each week Globally
##############################################################################
top_songs_each_week <- bigset %>% 
  filter(country == "GLOBAL", rank == 1) %>% 
  group_by(week_date) %>% 
  select(-rank)

View(top_songs_each_week)

# See how mnay weeks each song have been 1st Globally
top_songs_each_week %>% 
  group_by(track_name) %>% 
  count(track_name) %>% 
  arrange(desc(n))


##############################################################################
############### Looking into BIRDS OF A FEATHER
##############################################################################

# Specify the song name
song_name <- "BIRDS OF A FEATHER"

# Filter for the song and find its first appearance
song_first_appearance <- bigset %>%
  filter(track_name == song_name) %>%  # Filter for "BIRDS OF A FEATHER"
  arrange(week_date) %>%  # Sort by week_date to get chronological order
  slice_min(order_by = week_date, n = 1)  # Get the first appearance (earliest date)

# View the relevant details for the first appearance
song_first_appearance %>%
  dplyr::select(track_name, country, week_date)  # Use select to get the relevant columns



##############################################################################
################# Global Top 200 ranked
##############################################################################

# Step 1: Filter for Global songs
global_songs <- bigset %>%
  filter(country == "GLOBAL")  # Only select rows where the country is 'GLOBAL'

# Step 2: Group by track_id and track_name, sum streams, and count weeks in the global charts
global_songs_ranked <- global_songs %>%
  group_by(track_id, track_name) %>%
  summarise(
    total_streams = sum(streams, na.rm = TRUE),  # Sum the streams for each song
    weeks_in_chart = n_distinct(week_date),  # Count the number of distinct weeks the song was in the chart
    .groups = "drop"  # Drop grouping after summarising
  ) %>%
  arrange(desc(total_streams))  # Sort by total streams in descending order

# View the result
global_songs_ranked

print (n=15, global_songs_ranked)

##############################################################################
############## Where the track first appeared 
##############################################################################


library(dplyr)

# Step 1: Extract the earliest national chart appearance for each song
first_appearance <- bigset %>%
  filter(country != "GLOBAL") %>%
  group_by(track_id) %>%
  filter(week_date == min(week_date)) %>%
  ungroup()

# Step 2: Create a list showing the first week and countries where each song appeared
first_week_countries <- first_appearance %>%
  group_by(track_id, track_name, week_date) %>%
  summarise(countries = paste(unique(country), collapse = ", ")) %>%
  ungroup()

# Step 3: Filter for songs that later appear in the global chart
global_appearance <- bigset %>%
  filter(country == "GLOBAL") %>%
  select(track_id, week_date)

# Step 4: Join the first appearance with global chart data and filter for valid entries
first_to_global <- first_appearance %>%
  inner_join(global_appearance, by = "track_id", suffix = c("_national", "_global"), relationship = "many-to-many") %>%
  filter(week_date_national < week_date_global)

# Step 5: Count the frequency of each country being the first to chart a song before it reaches the global charts
country_frequency <- first_to_global %>%
  group_by(country) %>%
  summarise(frequency = n()) %>%
  arrange(desc(frequency))

# Output the first week and countries list
print(first_week_countries)

# Output the ranked list of countries
print(country_frequency)


############## Visualization

library(ggplot2)
library(forcats)
library(scales)

# Step 1: Get the top 20 countries
top_20_countries <- country_frequency %>%
  arrange(desc(frequency)) %>%
  head(20)

# Create the bar chart with flipped axis
ggplot(top_20_countries, aes(x = reorder(country, -frequency), y = frequency, 
                             fill = country == country[1])) +
  geom_bar(stat = 'identity', show.legend = FALSE) +
  scale_y_continuous(
    limits = c(0, max(top_20_countries$frequency) + 500),  # Start y-axis at 3000
    expand = c(0, 0), 
    labels = label_comma()
  ) +
  scale_fill_manual(values = c('darkgrey', 'red')) +  # Highlight the max country
  geom_text(aes(label = frequency), 
            vjust = -0.5, 
            size = 3, 
            color = "white") +
  labs(title = "Top 20 Countries with the Highest Frequency of Global Hits First Appearing in Their Charts",
       x = "Country", y = "Frequency") +
  theme_bw() +
  theme(
    plot.title = element_text(size = 14, hjust = 0.5),
    axis.text.x = element_text(angle = 45, hjust = 1),  # Rotate x-axis labels
    axis.title.y = element_blank()
  )





w##############################################################################
############## Time Lag 
##############################################################################







library(dplyr)
library(ggplot2)

# Convert dates to Date type
bigset <- bigset %>%
  mutate(week_date = as.Date(week_date),
         release_date = as.Date(release_date))

# Identify global chart entries
global_charts <- bigset %>%
  filter(country == "GLOBAL") %>%
  select(track_id, global_week_date = week_date)

# Merge with original data to find national chart entries
national_to_global <- bigset %>%
  inner_join(global_charts, by = "track_id") %>%
  filter(country != "GLOBAL") %>%
  mutate(days_to_global = as.numeric(week_date - global_week_date))

# Calculate average time lag for each country
country_avg_lag <- national_to_global %>%
  group_by(country) %>%
  summarise(avg_days_to_global = mean(days_to_global, na.rm = TRUE)) %>%
  arrange(avg_days_to_global)

# Lollipop chart
# Select Top 10, Middle 10, and Bottom 10 countries
country_filtered <- country_avg_lag %>%
  arrange(avg_days_to_global) %>%
  mutate(rank = row_number()) %>%
  filter(rank <= 15 | rank > (n() - 15) | (rank > (n()/2 - 5) & rank <= (n()/2 + 5)))
ft
# Create the lollipop chart
ggplot(country_filtered, aes(x = reorder(country, avg_days_to_global), y = avg_days_to_global, color = avg_days_to_global)) +
  geom_segment(aes(yend = 0, xend = country), color = "grey") +  # Lollipop stick
  geom_point(size = 4) +  # Lollipop head
  scale_color_gradient(low = "blue", high = "pink") +  # Gradient color
  scale_y_continuous(breaks = seq(-55, 30, by = 5)) +  # Set y-axis breaks every 5 units
  labs(title = "Average Days it takes a Song from National to Global Charts",
       x = "Country",
       y = "Days on Average",
       color = "Days to Global") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))  # Rotate x-axis labels for better readability


# Print ranked list
print(country_avg_lag)
print(n=72, country_avg_lag)
























  
  ################################################
############ CLOSENESS CENTRALITY #################
  ################################################
  ###############################################



install.packages("igraph")  
  library(dplyr)
  library(igraph)
  library(ggraph)
  library(countrycode)
  
  # Step 1: Extract first chart appearances
  first_appearances <- bigset %>%
    filter(country != "GLOBAL") %>%
    group_by(track_id, country) %>%
    summarize(first_week = min(week_date), .groups = 'drop')
  
  # Step 2: Identify country-to-country transitions
  transitions <- first_appearances %>%
    group_by(track_id) %>%
    arrange(first_week) %>%
    mutate(next_country = lead(country)) %>%
    filter(!is.na(next_country)) %>%
    ungroup() %>%
    count(country, next_country, name = "weight")
  
  # Step 3: Build the influence network
  g <- graph_from_data_frame(transitions, directed = TRUE)
  
  # Step 4: Calculate closeness centrality
  closeness_centrality <- closeness(g, mode = "out", normalized = TRUE)
  V(g)$closeness <- closeness_centrality

  # Step 4.1: Create a ranked dataframe of closeness centrality scores
  closeness_df <- data.frame(
    country = V(g)$name,
    closeness = closeness_centrality
  ) %>%
    arrange(closeness) %>%  # Sort in ascending order
    mutate(rank = row_number())  # Rank starts at 1 for lowest closeness
  
  # View the dataframe
  print(closeness_df)
  
  # Step 5: Prepare for visualization
  V(g)$continent <- countrycode(V(g)$name, "iso2c", "continent")
  V(g)$size <- 1 / V(g)$closeness * 10  # Node size based on influence
  V(g)$color <- ifelse(V(g)$closeness == min(V(g)$closeness), "red", "black")  # Red outline for most influential
  
  # Step 6: Visualize the network
  ggraph(g, layout = "fr") +
    geom_edge_link(aes(width = weight, color = weight), alpha = 0.8) +  # Edge color based on weight
    geom_node_point(aes(size = size, color = continent)) +  # Fully colored nodes
    geom_node_text(aes(label = name), repel = TRUE, size = 3, color = "black") +  # Ensure text is readable
    scale_edge_width(range = c(0.1, 2.5)) +
    scale_edge_color_gradient(low = "lightgrey", high = "black") +  # Gradient from grey to black
    scale_size_continuous(range = c(2, 10)) +
    theme_void() +
    theme(legend.position = "bottom") + 
  
   theme_void() +  
    labs(title = "Global Music Network",
         subtitle = "Tracking the spread of hit songs across countries based on first chart appearances") +
    theme(
      plot.title = element_text(size = 16, face = "bold", hjust = 0.5),
      plot.subtitle = element_text(size = 12, hjust = 0.5),
      legend.position = "right"
    )
  
  
  
  
  
  
  
  
  
  
  
  