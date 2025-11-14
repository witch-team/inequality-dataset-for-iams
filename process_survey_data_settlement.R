# Process Distributional Data created for the NAVIGATE project
# Author: Johannes Emmerling, data collection: Shouro Dasgupta, Simon Feindt, Daniele Malerba, Carolina Grottera
#
# This script processes inequality data with SETTLEMENT (urban/rural) disaggregation on top of deciles
# Key differences from process_survey_data.R:
#   - Processes both CSV and XLSX files from the settlement/ folder
#   - Includes settlement dimension (urban/rural) in the output
#   - Output format: year, iso3, var, element, settlement, dist, value
#   - Adds data quality checks for each file processed
#
require(data.table)
require(countrycode)
require(stringr)
require(tidyverse)
require(openxlsx)

folder <- "settlement"

#first get POTENTIAL list of variables
print("All variables in the template")
print(unique(read.xlsx(file.path(folder, str_subset(list.files(path = folder), "EMPTY")))$VARIABLE))


# Get list of both CSV and XLSX files
csv_files <- list.files(path = folder, pattern = "Inequality Input Data Template.*Settlement.*\\.csv$")
xlsx_files <- list.files(path = folder, pattern = "Inequality Input Data Template.*Settlement.*\\.xlsx$")
survey_inequality_filelist <- c(csv_files, xlsx_files)
# Filter out EMPTY templates
survey_inequality_filelist <- survey_inequality_filelist[!str_detect(survey_inequality_filelist, "EMPTY") & !str_detect(survey_inequality_filelist, "template")]

cat("Found", length(survey_inequality_filelist), "files to process:\n")
print(survey_inequality_filelist)

#.file <- survey_inequality_filelist[1]
allvars <- c("expcat_input", "incomecat", "savings_rate", "wealth_share", "educat", "inequality_index", "household_size", "expenditure_decile", "income_decile")

for (.file in survey_inequality_filelist) {
  cat("\n--- Processing:", .file, "---\n")

  # Read CSV or XLSX based on file extension
  if(str_detect(.file, "\\.csv$")) {
    data <- fread(file.path(folder, .file), header = T)
  } else if(str_detect(.file, "\\.xlsx$")) {
    data <- read.xlsx(file.path(folder, .file)) %>% as.data.table()
  } else {
    warning("Unknown file format for ", .file)
    next
  }

  # Basic checks
  cat("  Dimensions:", nrow(data), "rows x", ncol(data), "columns\n")
  required_cols <- c("MODEL", "SCENARIO", "REGION", "VARIABLE", "UNIT")
  missing_cols <- setdiff(required_cols, names(data))
  if(length(missing_cols) > 0) {
    warning("Missing required columns in ", .file, ": ", paste(missing_cols, collapse = ", "))
    next
  }

  # Check if Urban/Rural are column names (alternative format)
  has_settlement_cols <- "Urban" %in% names(data) || "Rural" %in% names(data)
  cat("  Data format:", ifelse(has_settlement_cols, "Settlement as columns", "Settlement in VARIABLE"), "\n")

  #specific command for EU
  if(.file=="Inequality Input Data Template CMCC_EU.csv") data <- data %>% filter(REGION!="FRA") #%>% filter(!str_detect(VARIABLE, "Emissions"))

  # Convert all potential year/data columns to character to avoid type conflicts
  data_cols <- setdiff(names(data), c("MODEL", "SCENARIO", "REGION", "VARIABLE", "UNIT"))
  # Remove empty columns (like V8, X8, etc.)
  data <- data %>% select(-str_subset(names(data), "^[VX][0-9]+"))

  data_output <- data %>% select(-MODEL, -SCENARIO, -UNIT) %>% rename(iso3=REGION) %>% mutate(dist=ifelse(str_detect(VARIABLE, "Inequality Index"), "0", str_extract(VARIABLE, "D[0-9].*$")))
  data_output <- data_output %>% mutate(var=case_when(str_detect(VARIABLE, "Expenditure Share") ~ "expcat_input", str_detect(VARIABLE, "Income Share") ~ "incomecat", str_detect(VARIABLE, "Savings Rate") ~ "savings_rate", str_detect(VARIABLE, "Wealth Share") ~ "wealth_share", str_detect(VARIABLE, "Education") ~ "educat", str_detect(VARIABLE, "Wage Premium") ~ "wage_premium", str_detect(VARIABLE, "Expenditure Decile") ~ "expenditure_decile",  str_detect(VARIABLE, "Income Decile") ~ "income_decile", str_detect(VARIABLE, "Inequality Index") ~ "inequality_index", str_detect(VARIABLE, "Household Size") ~ "household_size", str_detect(VARIABLE, "Emissions per capita") ~ "emissions_per_capita"))
  data_output <- data_output %>% filter(!str_detect(VARIABLE, "Meat")) #for now don't separate out meat consumption
  data_output <- data_output %>% mutate(element=case_when(str_detect(VARIABLE, "Housing") ~ "energy_housing", str_detect(VARIABLE, "Transportation") ~ "energy_transportation", str_detect(VARIABLE, "Food") ~ "food", str_detect(VARIABLE, "Expenditure Share|Other") ~ "other", str_detect(VARIABLE, "Labour") ~ "labour", str_detect(VARIABLE, "Capital") ~ "capital", str_detect(VARIABLE, "Transfers") ~ "transfers", str_detect(VARIABLE, "Income Share|Other") ~ "other", str_detect(VARIABLE, "Under 15") ~ "Under 15", str_detect(VARIABLE, "No Education") ~ "No education", str_detect(VARIABLE, "Primary Education") ~ "Primary Education", str_detect(VARIABLE, "Secondary Education") ~ "Secondary Education", str_detect(VARIABLE, "Tertiary Education") ~ "Tertiary Education", str_detect(VARIABLE, "Gini") ~ "gini", str_detect(VARIABLE, "Absolute Poverty") ~ "absolute_poverty"))
  #create a settlement variable - check if it's in VARIABLE or as columns
  if(has_settlement_cols) {
    # Format 1: Settlement as columns (Urban, Rural)
    # These columns contain the actual values for urban and rural settlements
    settlement_cols <- intersect(c("Urban", "Rural"), names(data_output))

    data_output <- data_output %>%
      select(-VARIABLE) %>%
      pivot_longer(cols = all_of(settlement_cols),
                   names_to = "settlement",
                   values_to = "value") %>%
      mutate(settlement = tolower(settlement)) %>%
      as.data.frame()

    # Try to extract year from filename or use "latest" as default
    year_match <- str_extract(.file, "[0-9]{4}")
    default_year <- ifelse(is.na(year_match), "latest", year_match)
    data_output <- data_output %>% mutate(year = default_year)

  } else {
    # Format 2: Settlement in VARIABLE field (original format)
    data_output <- data_output %>% mutate(settlement=case_when(str_detect(VARIABLE, "Urban") ~ "urban", str_detect(VARIABLE, "Rural") ~ "rural"))
    data_output <- data_output %>% select(-VARIABLE) %>% pivot_longer(cols = setdiff(names(data_output), c("iso3", "VARIABLE", "dist", "settlement", "var", "element")), names_to = "year") %>% as.data.frame()
  }

  # Ensure consistent column order
  data_output <- data_output %>% select(year, iso3, var, element, settlement, dist, value)

  # Standardize units: Convert fractions to percentages where appropriate
  # Check if values for shares/rates are in fraction format (0-1) instead of percentage (0-100)
  # This applies to: expcat_input, incomecat, savings_rate, wealth_share, educat, expenditure_decile, income_decile
  share_vars <- c("expcat_input", "incomecat", "savings_rate", "wealth_share", "educat", "expenditure_decile", "income_decile")
  for(check_var in share_vars) {
    var_data <- data_output[data_output$var == check_var & !is.na(data_output$value), ]
    if(nrow(var_data) > 0) {
      max_val <- max(var_data$value, na.rm = TRUE)
      min_val <- min(var_data$value, na.rm = TRUE)
      # If max value is <= 1.5, assume it's in fraction format and convert to percentage
      if(max_val <= 1.5) {
        cat("  Converting", check_var, "from fractions to percentages (range:", round(min_val, 3), "-", round(max_val, 3), ")\n")
        # Use which() to avoid NA issues in logical indexing
        idx <- which(data_output$var == check_var & !is.na(data_output$var))
        data_output$value[idx] <- data_output$value[idx] * 100
      }
    }
  }

  # Data quality checks
  cat("  Variables found:", paste(unique(data_output$var), collapse = ", "), "\n")
  missing_vars <- allvars[!(allvars %in% unique(data_output$var))]
  if(length(missing_vars) > 0) {
    cat("  WARNING - Variables missing:", paste(missing_vars, collapse = ", "), "\n")
  }

  # Check for settlement dimension
  settlements <- unique(data_output$settlement)
  cat("  Settlements found:", paste(settlements[!is.na(settlements)], collapse = ", "), "\n")
  if(all(is.na(settlements))) {
    warning("No settlement dimension found in ", .file)
  }

  # Check for data completeness
  cat("  Total observations:", nrow(data_output), "\n")
  cat("  Non-NA values:", sum(!is.na(data_output$value)), "\n")

  # Validate that expenditure/income deciles sum to ~100% per settlement
  for(decile_var in c("expenditure_decile", "income_decile")) {
    if(decile_var %in% data_output$var) {
      decile_sums <- data_output %>%
        filter(var == decile_var & !is.na(value) & !is.na(settlement)) %>%
        group_by(iso3, settlement) %>%
        summarize(total = sum(value, na.rm=TRUE), .groups='drop')

      problematic <- decile_sums %>% filter(abs(total - 100) > 5)
      if(nrow(problematic) > 0) {
        cat("  WARNING - ", decile_var, " sums deviate from 100%:\n")
        for(i in 1:nrow(problematic)) {
          cat("    ", problematic$iso3[i], "-", problematic$settlement[i], ": sum =", round(problematic$total[i], 2), "%\n")
        }
      }
    }
  }
  if(.file==survey_inequality_filelist[1]){
    data_output_allcountries <- data_output
    data_input_format <- data[,1:6]
  }else{
    data_output_allcountries <- rbind(data_output_allcountries, data_output)
    data_input_format <- rbind(data_input_format, data[,1:6], fill=T)
  } 
}

# STORE DATA combined CSV file
fwrite(data_output_allcountries, file = file.path(folder, "deciles_data_settlement.csv"))

# Summary statistics
cat("\n=== FINAL SUMMARY ===\n")
cat("Total files processed:", length(survey_inequality_filelist), "\n")
cat("Total countries:", length(unique(data_output_allcountries$iso3)), "-", paste(unique(data_output_allcountries$iso3), collapse = ", "), "\n")
cat("Total observations:", nrow(data_output_allcountries), "\n")
cat("Variables covered:", paste(unique(data_output_allcountries$var), collapse = ", "), "\n")
cat("Settlements covered:", paste(unique(data_output_allcountries$settlement[!is.na(data_output_allcountries$settlement)]), collapse = ", "), "\n")
cat("Year range:", min(data_output_allcountries$year, na.rm=T), "-", max(data_output_allcountries$year, na.rm=T), "\n")
cat("Output file: settlement/deciles_data_settlement.csv\n")

#Show list of countries and variables
ggplot(data_input_format %>% filter(str_detect(VARIABLE, "D10")) %>% group_by(REGION, VARIABLE) %>% summarize(avail=length(UNIT)) %>% mutate(VARIABLE=gsub("\\|D10", "", VARIABLE)), aes(REGION,VARIABLE, fill = avail)) + geom_tile() + theme_minimal() + theme(axis.text.x = element_text(angle=90, vjust = 0.5)) + guides(fill="none") 
ggsave(path = folder, "Countries and Variables.png", width = 10, height=8)
#show expenditure shares - all curves per country, solid=urban, dashed=rural, colors=element
ggplot(data_output_allcountries %>%
         filter(element %in% c("energy_housing", "energy_transportation", "food", "other") &
                var=="expcat_input" & !is.na(settlement)) %>%
         mutate(decile=(as.numeric(gsub("D", "", dist))))) +
  geom_line(aes(decile, value, color=element, linetype=settlement), linewidth=0.8) +
  facet_wrap(. ~ iso3, scales = "free_y", ncol=3) +
  theme_minimal() +
  theme(legend.position = "bottom") +
  labs(x="Decile", y="Expenditure share [%]",
       color="Expenditure type", linetype="Settlement") +
  scale_x_continuous(breaks=seq(1,10)) +
  scale_linetype_manual(values=c("rural"="dashed", "urban"="solid"))
ggsave(path = folder, "Energy Expenditure Shares.png", width = 12, height=10)
#Show deciles - separate by settlement (urban/rural)
ggplot(data_output_allcountries %>% filter(var=="income_decile" & !is.na(settlement)) %>% mutate(decile=(as.numeric(gsub("D", "", dist))))) + geom_line(aes(decile, value, color=settlement)) + facet_wrap(. ~ iso3, scales = "free_y") + theme_minimal() + theme(legend.position = "bottom") + labs(x="Decile", y="Income decile share [%]") + scale_x_continuous(breaks=seq(1,10))
ggsave(path = folder, "Income deciles.png", width = 10, height=8)
ggplot(data_output_allcountries %>% filter(var=="expenditure_decile" & !is.na(settlement)) %>% mutate(decile=(as.numeric(gsub("D", "", dist))))) + geom_line(aes(decile, value, color=settlement)) + facet_wrap(. ~ iso3, scales = "free_y") + theme_minimal() + theme(legend.position = "bottom") + labs(x="Decile", y="Expenditure decile share [%]") + scale_x_continuous(breaks=seq(1,10))
ggsave(path = folder, "Expenditure deciles.png", width = 10, height=8)


# ============================================================================
# INEQUALITY ANALYSIS: THEIL DECOMPOSITION
# ============================================================================

cat("\n\n=== THEIL DECOMPOSITION ANALYSIS ===\n\n")

# Function to calculate Theil T index
calculate_theil <- function(shares, weights=NULL) {
  # shares: income/expenditure shares (proportions, should sum to 1)
  # weights: population weights (if NULL, equal weights assumed)
  if(is.null(weights)) weights <- rep(1/length(shares), length(shares))
  weights <- weights / sum(weights)  # normalize to sum to 1
  shares <- shares / sum(shares)     # normalize shares to sum to 1

  # Theil T = sum(share_i * log(share_i / weight_i))
  # Remove zero shares to avoid log(0)
  valid <- shares > 0 & weights > 0
  if(sum(valid) == 0) return(NA)

  theil <- sum(shares[valid] * log(shares[valid] / weights[valid]))
  return(theil)
}

# Function to decompose Theil between and within groups
theil_decomposition <- function(data_df) {
  # data_df should have: group, decile, share, pop_share
  # Calculate total Theil
  total_shares <- data_df$share * data_df$pop_share
  total_theil <- calculate_theil(total_shares, data_df$pop_share)

  # Between-group inequality (treating each group as having its mean income)
  group_means <- aggregate(share ~ group, data_df, function(x) weighted.mean(x, data_df$pop_share[data_df$group == data_df$group[1]]))
  group_pops <- aggregate(pop_share ~ group, data_df, sum)
  between_theil <- calculate_theil(group_means$share, group_pops$pop_share)

  # Within-group inequality
  within_theil <- 0
  for(g in unique(data_df$group)) {
    group_data <- data_df[data_df$group == g, ]
    group_share <- sum(group_data$share * group_data$pop_share) / sum(group_data$pop_share)
    group_pop_share <- sum(group_data$pop_share)
    if(group_share > 0 && group_pop_share > 0) {
      group_theil <- calculate_theil(group_data$share, group_data$pop_share)
      within_theil <- within_theil + group_share * group_theil
    }
  }

  return(list(total=total_theil, between=between_theil, within=within_theil))
}

# Prepare data for Theil analysis
# Assume equal population per decile (10% each), but we'll need to aggregate by settlement
# For simplicity, assume 50-50 urban-rural split (can be adjusted with actual data)

theil_results <- data.frame()

for(country in unique(data_output_allcountries$iso3)) {
  cat("Analyzing", country, "...\n")

  country_data <- data_output_allcountries %>% filter(iso3 == country & !is.na(settlement))

  # 1. Income distribution
  income_data <- country_data %>%
    filter(var == "income_decile") %>%
    mutate(share = value / 100,  # convert percentage to proportion
           pop_share = 0.1 * 0.5,  # 10% per decile, 50% per settlement
           group = settlement) %>%
    filter(!is.na(share) & share > 0)

  if(nrow(income_data) > 0) {
    income_theil <- tryCatch({
      # Simple Theil calculation across all deciles
      shares <- income_data$share
      theil <- calculate_theil(shares)
      theil
    }, error = function(e) NA)

    theil_results <- rbind(theil_results, data.frame(
      iso3 = country,
      variable = "income",
      theil = income_theil
    ))
  }

  # 2. Expenditure distribution
  exp_data <- country_data %>%
    filter(var == "expenditure_decile") %>%
    mutate(share = value / 100,
           pop_share = 0.1 * 0.5,
           group = settlement) %>%
    filter(!is.na(share) & share > 0)

  if(nrow(exp_data) > 0) {
    exp_theil <- tryCatch({
      shares <- exp_data$share
      calculate_theil(shares)
    }, error = function(e) NA)

    theil_results <- rbind(theil_results, data.frame(
      iso3 = country,
      variable = "expenditure",
      theil = exp_theil
    ))
  }

  # 3. Energy expenditure shares (housing and transportation)
  for(energy_type in c("energy_housing", "energy_transportation")) {
    energy_data <- country_data %>%
      filter(var == "expcat_input" & element == energy_type) %>%
      mutate(share = value / 100,
             pop_share = 0.1 * 0.5,
             group = settlement) %>%
      filter(!is.na(share) & share > 0)

    if(nrow(energy_data) > 0) {
      energy_theil <- tryCatch({
        shares <- energy_data$share
        calculate_theil(shares)
      }, error = function(e) NA)

      theil_results <- rbind(theil_results, data.frame(
        iso3 = country,
        variable = energy_type,
        theil = energy_theil
      ))
    }
  }
}

cat("\n--- Theil Index Results ---\n")
print(theil_results %>% pivot_wider(names_from = variable, values_from = theil))


# Plot Theil indices
ggplot(theil_results, aes(x=iso3, y=theil, fill=variable)) +
  geom_bar(stat="identity", position="dodge") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle=45, hjust=1)) +
  labs(x="Country", y="Theil T Index", fill="Variable",
       title="Inequality Measures (Theil T Index)") +
  scale_fill_brewer(palette="Set2")
ggsave(path = folder, "Theil_decomposition.png", width = 10, height=6)


# ============================================================================
# BETWEEN-WITHIN DECOMPOSITION (Urban vs Rural)
# ============================================================================

cat("\n\n=== BETWEEN-WITHIN DECOMPOSITION (Urban vs Rural) ===\n\n")

decomp_results <- data.frame()

for(country in unique(data_output_allcountries$iso3)) {
  cat("Decomposing", country, "...\n")

  country_data <- data_output_allcountries %>% filter(iso3 == country & !is.na(settlement))

  # Get actual population weights from household_size data
  # household_size gives persons per household in each decile
  # Each decile has 10% of households, so population = 10% * household_size
  hhsize_data <- country_data %>%
    filter(var == "household_size") %>%
    mutate(decile_num = as.numeric(gsub("D", "", dist)),
           hhsize = value) %>%
    select(settlement, decile_num, hhsize)

  if(nrow(hhsize_data) > 0) {
    # Calculate population in each decile (assuming 10% of households per decile)
    hhsize_data <- hhsize_data %>%
      mutate(pop = hhsize * 0.1)  # 10% of households * household size

    # Total population by settlement
    urban_pop <- sum(hhsize_data$pop[hhsize_data$settlement == "urban"], na.rm=TRUE)
    rural_pop <- sum(hhsize_data$pop[hhsize_data$settlement == "rural"], na.rm=TRUE)
    total_pop <- urban_pop + rural_pop

    if(total_pop > 0) {
      urban_pop_share <- urban_pop / total_pop
      rural_pop_share <- rural_pop / total_pop
      cat("  Population shares - Urban:", round(urban_pop_share * 100, 1), "%, Rural:", round(rural_pop_share * 100, 1), "%\n")
    } else {
      # Fallback if no household size data
      urban_pop_share <- 0.5
      rural_pop_share <- 0.5
      cat("  WARNING: No household size data, using 50-50 split\n")
    }
  } else {
    # Fallback if no household size data
    urban_pop_share <- 0.5
    rural_pop_share <- 0.5
    cat("  WARNING: No household size data, using 50-50 split\n")
  }

  # For each variable type
  for(var_type in c("income_decile", "expenditure_decile")) {

    var_data <- country_data %>%
      filter(var == var_type) %>%
      mutate(decile_num = as.numeric(gsub("D", "", dist)),
             share = value / 100) %>%  # Share of total income/expenditure in this decile
      filter(!is.na(share) & !is.na(decile_num)) %>%
      arrange(settlement, decile_num)

    if(nrow(var_data) < 4) next  # Need data for both settlements

    # Check data completeness
    n_urban <- sum(var_data$settlement == "urban")
    n_rural <- sum(var_data$settlement == "rural")
    if(n_urban < 10 || n_rural < 10) {
      cat("  WARNING: Incomplete data for", var_type, "(urban:", n_urban, ", rural:", n_rural, ")\n")
    }

    urban_data <- var_data %>% filter(settlement == "urban")
    rural_data <- var_data %>% filter(settlement == "rural")

    if(nrow(urban_data) == 0 || nrow(rural_data) == 0) next

    # Normalize shares to ensure they sum to their respective population shares
    # Each decile represents 10% of the population within its settlement
    # Income share of each group (urban/rural) in total population
    urban_total_income_share <- sum(urban_data$share, na.rm=TRUE)
    rural_total_income_share <- sum(rural_data$share, na.rm=TRUE)
    total_income <- urban_total_income_share + rural_total_income_share

    # Normalize so urban + rural = 1
    urban_total_income_share <- urban_total_income_share / total_income
    rural_total_income_share <- rural_total_income_share / total_income

    # Mean income per capita in each group
    urban_mean_income <- urban_total_income_share / urban_pop_share
    rural_mean_income <- rural_total_income_share / rural_pop_share

    # Total Theil: Use actual population weights from household size
    # Get household size for each decile to calculate population weights
    var_data_with_pop <- var_data %>%
      left_join(hhsize_data, by = c("settlement", "decile_num")) %>%
      mutate(pop_weight = ifelse(!is.na(hhsize), hhsize * 0.1, 0.1 * 0.5))  # Fallback to equal if no hhsize

    # Normalize population weights to sum to 1
    var_data_with_pop <- var_data_with_pop %>%
      mutate(pop_weight = pop_weight / sum(pop_weight, na.rm=TRUE))

    income_shares <- var_data_with_pop$share / total_income
    pop_weights <- var_data_with_pop$pop_weight

    total_theil <- calculate_theil(income_shares, pop_weights)

    # Between-group Theil: Compare urban vs rural mean incomes
    # Treating all urban as having urban mean, all rural as having rural mean
    group_income_shares <- c(urban_total_income_share, rural_total_income_share)
    group_pop_weights <- c(urban_pop_share, rural_pop_share)
    between_theil <- calculate_theil(group_income_shares, group_pop_weights)

    # Within-group Theil: Inequality within urban + within rural
    # For urban: each urban decile has 10% of urban population
    urban_theil <- calculate_theil(urban_data$share / sum(urban_data$share))
    rural_theil <- calculate_theil(rural_data$share / sum(rural_data$share))

    # Weight by group's income share (not population share)
    within_theil <- urban_total_income_share * urban_theil + rural_total_income_share * rural_theil

    # Verification: total should equal between + within
    verification_diff <- abs(total_theil - (between_theil + within_theil))
    if(verification_diff > 0.001) {
      cat("  WARNING: Decomposition doesn't add up for", var_type, "- difference:", verification_diff, "\n")
    }

    # Store results
    decomp_results <- rbind(decomp_results, data.frame(
      iso3 = country,
      variable = var_type,
      total = total_theil,
      between = between_theil,
      within = within_theil,
      between_pct = (between_theil / total_theil) * 100,
      within_pct = (within_theil / total_theil) * 100,
      urban_income_share = urban_total_income_share,
      rural_income_share = rural_total_income_share,
      verification_error = verification_diff
    ))
  }

  # Energy expenditure shares
  for(energy_type in c("energy_housing", "energy_transportation")) {

    energy_data <- country_data %>%
      filter(var == "expcat_input" & element == energy_type) %>%
      mutate(decile_num = as.numeric(gsub("D", "", dist)),
             share = value / 100) %>%
      filter(!is.na(share) & !is.na(decile_num)) %>%
      arrange(settlement, decile_num)

    if(nrow(energy_data) < 4) next

    # Check data completeness
    n_urban <- sum(energy_data$settlement == "urban")
    n_rural <- sum(energy_data$settlement == "rural")
    if(n_urban < 10 || n_rural < 10) {
      cat("  WARNING: Incomplete data for", energy_type, "(urban:", n_urban, ", rural:", n_rural, ")\n")
    }

    urban_data <- energy_data %>% filter(settlement == "urban")
    rural_data <- energy_data %>% filter(settlement == "rural")

    if(nrow(urban_data) == 0 || nrow(rural_data) == 0) next

    # Same logic as for income/expenditure deciles
    urban_total_share <- sum(urban_data$share, na.rm=TRUE)
    rural_total_share <- sum(rural_data$share, na.rm=TRUE)
    total_share <- urban_total_share + rural_total_share

    # Normalize
    urban_total_share <- urban_total_share / total_share
    rural_total_share <- rural_total_share / total_share

    # Total Theil
    pop_weights <- rep(0.1 * 0.5, nrow(energy_data))
    income_shares <- c(
      urban_data$share / total_share,
      rural_data$share / total_share
    )
    total_theil <- calculate_theil(income_shares, pop_weights)

    # Between-group
    group_shares <- c(urban_total_share, rural_total_share)
    group_weights <- c(urban_pop_share, rural_pop_share)
    between_theil <- calculate_theil(group_shares, group_weights)

    # Within-group
    urban_theil <- calculate_theil(urban_data$share / sum(urban_data$share))
    rural_theil <- calculate_theil(rural_data$share / sum(rural_data$share))
    within_theil <- urban_total_share * urban_theil + rural_total_share * rural_theil

    # Verification
    verification_diff <- abs(total_theil - (between_theil + within_theil))
    if(verification_diff > 0.001) {
      cat("  WARNING: Decomposition doesn't add up for", energy_type, "- difference:", verification_diff, "\n")
    }

    decomp_results <- rbind(decomp_results, data.frame(
      iso3 = country,
      variable = energy_type,
      total = total_theil,
      between = between_theil,
      within = within_theil,
      between_pct = (between_theil / total_theil) * 100,
      within_pct = (within_theil / total_theil) * 100,
      urban_income_share = urban_total_share,
      rural_income_share = rural_total_share,
      verification_error = verification_diff
    ))
  }
}

cat("\n--- Between-Within Decomposition Results ---\n")
print(decomp_results)

# Reshape for plotting
decomp_long <- decomp_results %>%
  select(iso3, variable, between, within) %>%
  pivot_longer(cols = c(between, within), names_to = "component", values_to = "theil")

# Create better labels
decomp_long <- decomp_long %>%
  mutate(variable_label = case_when(
    variable == "income_decile" ~ "Income",
    variable == "expenditure_decile" ~ "Expenditure",
    variable == "energy_housing" ~ "Housing Energy",
    variable == "energy_transportation" ~ "Transport Energy",
    TRUE ~ variable
  ))

# Plot decomposition
ggplot(decomp_long, aes(x=iso3, y=theil, fill=component)) +
  geom_bar(stat="identity", position="stack") +
  facet_wrap(~ variable_label, scales="free_y", ncol=2) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle=45, hjust=1),
        legend.position = "bottom") +
  labs(x="Country", y="Theil T Index", fill="Component",
       title="Inequality Decomposition: Between vs Within Urban-Rural",
       subtitle="Stacked bars show total inequality decomposed into between-group and within-group components") +
  scale_fill_manual(values=c("between"="#E69F00", "within"="#56B4E9"),
                    labels=c("between"="Between Urban/Rural", "within"="Within Urban/Rural"))
ggsave(path = folder, "Theil_between_within_decomposition.png", width = 12, height=8)


