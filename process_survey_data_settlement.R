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

# Open PDF for ALL comprehensive plots
pdf(file=file.path(folder, "settlement_inequality_analysis.pdf"), width = 12, height = 10)

#first get POTENTIAL list of variables
print("All variables in the template")
print(unique(read.xlsx(file.path(folder, str_subset(list.files(path = folder), "EMPTY")))$VARIABLE))


# Get list of both CSV and XLSX files
csv_files <- list.files(path = folder, pattern = "Inequality Input Data Template.*Settlement.*\\.csv$")
xlsx_files <- list.files(path = folder, pattern = "Inequality Input Data Template.*Settlement.*\\.xlsx$")
survey_inequality_filelist <- c(csv_files, xlsx_files)
# Filter out EMPTY templates, temp files, and superseded EUv2 draft
survey_inequality_filelist <- survey_inequality_filelist[
  !str_detect(survey_inequality_filelist, "EMPTY") &
  !str_detect(survey_inequality_filelist, "template") &
  !str_detect(survey_inequality_filelist, "^~\\$") &
  !str_detect(survey_inequality_filelist, "Settlement EUv2\\.csv$")
]

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

  # Remove AVERAGE label column if present (string column, not numeric data)
  if("AVERAGE" %in% names(data)) {
    data <- data %>% select(-AVERAGE)
    cat("  Removed AVERAGE label column\n")
  }

  # Basic checks
  cat("  Dimensions:", nrow(data), "rows x", ncol(data), "columns\n")

  # Remove duplicate rows (some files have duplicate entries)
  # For exact duplicates, keep unique rows
  data_before <- nrow(data)
  data <- unique(data)
  if(nrow(data) < data_before) {
    cat("  Removed", data_before - nrow(data), "exact duplicate rows\n")
  }

  # For duplicate REGION+VARIABLE combinations with different values, take the mean
  dup_check <- data[, .N, by=c("REGION", "VARIABLE")]
  if(any(dup_check$N > 1)) {
    n_dup <- sum(dup_check$N > 1)
    data <- data[, lapply(.SD, function(x) if(is.numeric(x)) mean(x, na.rm=TRUE) else first(x)),
                 by=c("REGION", "VARIABLE"),
                 .SDcols = setdiff(names(data), c("REGION", "VARIABLE"))]
    cat("  Averaged", n_dup, "duplicate REGION+VARIABLE combinations\n")
  }

  # Normalise column names: ISO3 -> REGION (template used ISO3 historically)
  if("ISO3" %in% names(data) & !"REGION" %in% names(data)) {
    data <- data %>% rename(REGION = ISO3)
    cat("  Renamed ISO3 -> REGION\n")
  }

  # Add UNIT column if missing (some files don't have it)
  if(!"UNIT" %in% names(data)) {
    data$UNIT <- NA
    cat("  Added missing UNIT column\n")
  }

  required_cols <- c("MODEL", "SCENARIO", "REGION", "VARIABLE")
  missing_cols <- setdiff(required_cols, names(data))
  if(length(missing_cols) > 0) {
    warning("Missing required columns in ", .file, ": ", paste(missing_cols, collapse = ", "))
    next
  }

  # Detect format:
  #   1. Template xlsx:       D1-D10 as value columns, settlement in VARIABLE name
  #   2. EU/CSV old (EU.csv): Urban/Rural as value columns, decile in VARIABLE (e.g. "Savings Rate|D1")
  #   3. EU/CSV new (EUv2):   year columns (e.g. "2015"), settlement+decile in VARIABLE (e.g. "...Rural|D1")
  has_decile_cols    <- any(str_detect(names(data), "^D[0-9]+$"))
  has_settlement_cols <- all(c("Urban", "Rural") %in% names(data)) & !has_decile_cols
  cat("  Data format:", ifelse(has_decile_cols, "Template (D1-D10 columns)",
                        ifelse(has_settlement_cols, "EU/CSV old (Urban/Rural columns)",
                               "EU/CSV new (year columns)")), "\n")

  #specific command for EU
  #if(.file=="Inequality Input Data Template CMCC_EU.csv") data <- data %>% filter(REGION!="FRA") #%>% filter(!str_detect(VARIABLE, "Emissions"))

  # Convert all potential year/data columns to character to avoid type conflicts
  data_cols <- setdiff(names(data), c("MODEL", "SCENARIO", "REGION", "VARIABLE", "UNIT"))
  # Remove empty columns (like V8, X8, etc.)
  data <- data %>% select(-str_subset(names(data), "^[VX][0-9]+"))

  data_output <- data %>% select(-MODEL, -SCENARIO, -UNIT) %>% rename(iso3=REGION) %>% mutate(dist=ifelse(str_detect(VARIABLE, "Inequality Index"), "0", str_extract(VARIABLE, "D[0-9].*$")))
  data_output <- data_output %>% mutate(var=case_when(str_detect(VARIABLE, "Expenditure Share") ~ "expcat_input", str_detect(VARIABLE, "Income Share") ~ "incomecat", str_detect(VARIABLE, "Savings Rate") ~ "savings_rate", str_detect(VARIABLE, "Wealth Share") ~ "wealth_share", str_detect(VARIABLE, "Education") ~ "educat", str_detect(VARIABLE, "Wage Premium") ~ "wage_premium", str_detect(VARIABLE, "Expenditure Decile") ~ "expenditure_decile",  str_detect(VARIABLE, "Income Decile") ~ "income_decile", str_detect(VARIABLE, "Inequality Index") ~ "inequality_index", str_detect(VARIABLE, "Equivalence Household Size") ~ "equivalence_household_size", str_detect(VARIABLE, "Household Size") ~ "household_size", str_detect(VARIABLE, "Emissions per capita") ~ "emissions_per_capita"))
  data_output <- data_output %>% filter(!str_detect(VARIABLE, "Meat")) #for now don't separate out meat consumption
  data_output <- data_output %>% mutate(element=case_when(str_detect(VARIABLE, "Housing") ~ "energy_housing", str_detect(VARIABLE, "Transportation") ~ "energy_transportation", str_detect(VARIABLE, "Food") ~ "food", str_detect(VARIABLE, "Other") ~ "other", str_detect(VARIABLE, "Labour") ~ "labour", str_detect(VARIABLE, "Capital") ~ "capital", str_detect(VARIABLE, "Transfers") ~ "transfers", str_detect(VARIABLE, "Under 15") ~ "Under 15", str_detect(VARIABLE, "No Education") ~ "No education", str_detect(VARIABLE, "Primary Education") ~ "Primary Education", str_detect(VARIABLE, "Secondary Education") ~ "Secondary Education", str_detect(VARIABLE, "Tertiary Education") ~ "Tertiary Education", str_detect(VARIABLE, "Gini") ~ "gini", str_detect(VARIABLE, "Absolute Poverty") ~ "absolute_poverty"))
  if(has_settlement_cols) {
    # EU/CSV old format: Urban/Rural are value columns, no settlement in VARIABLE
    # Drop VARIABLE (decile already extracted into dist above), pivot Urban/Rural → settlement
    data_output <- data_output %>% select(-VARIABLE)
  } else {
    # Template and EU/CSV new: settlement is embedded in VARIABLE name
    data_output <- data_output %>%
      mutate(settlement = case_when(str_detect(VARIABLE, "Urban") ~ "urban",
                                    str_detect(VARIABLE, "Rural") ~ "rural")) %>%
      select(-VARIABLE)
  }

  if(has_decile_cols) {
    # Template xlsx format: D1-D10 are value columns, "value" column for non-decile rows
    data_output <- data_output %>%
      pivot_longer(cols = c(matches("^D[0-9]+$"), any_of("value")), names_to = "year") %>%
      filter(!is.na(value)) %>%
      mutate(dist = case_when(var == "inequality_index" ~ "0",
                              !is.na(dist) ~ dist,
                              str_detect(year, "^D[0-9]+$") ~ year,
                              TRUE ~ "0")) %>%
      as.data.frame()
  } else if(has_settlement_cols) {
    # EU/CSV old format: pivot Urban/Rural into settlement column, default year to 2015
    data_output <- data_output %>%
      pivot_longer(cols = c("Urban", "Rural"), names_to = "settlement", values_to = "value") %>%
      mutate(settlement = tolower(settlement),
             year = "2015") %>%
      filter(!is.na(value)) %>%
      as.data.frame()
  } else {
    # EU/CSV new format: year columns contain actual years, settlement already in VARIABLE
    data_output <- data_output %>%
      pivot_longer(cols = setdiff(names(data_output), c("iso3", "dist", "settlement", "var", "element")),
                   names_to = "year") %>%
      as.data.frame()
  }

  # Ensure consistent column order
  data_output <- data_output %>% select(year, iso3, var, element, settlement, dist, value)

  # Convert country names to ISO3C codes only if file ends with " EU"
  if(str_detect(.file, " EU")) {
    data_output <- data_output %>%
      mutate(iso3 = countrycode(iso3, origin = "country.name", destination = "iso3c",
                                 custom_match = c("Slovak Republic" = "SVK")))
    cat("  Converted country names to ISO3C codes\n")
  }

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
  # For EU files, convert REGION in raw data before storing in data_input_format
  if(str_detect(.file, " EU")) {
    data <- data %>%
      mutate(REGION = countrycode(REGION, origin = "country.name", destination = "iso3c",
                                   custom_match = c("Slovak Republic" = "SVK"), warn = FALSE))
  }
  if(.file==survey_inequality_filelist[1]){
    data_output_allcountries <- data_output
    data_input_format <- data %>% select(any_of(c("REGION", "VARIABLE", "UNIT")))
  }else{
    data_output_allcountries <- rbind(data_output_allcountries, data_output)
    data_input_format <- rbind(data_input_format,
                               data %>% select(any_of(c("REGION", "VARIABLE", "UNIT"))),
                               fill = TRUE)
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
# Get all template variables for the y-axis
all_template_vars <- unique(read.xlsx(file.path(folder, str_subset(list.files(path = folder), "EMPTY")))$VARIABLE)
# Clean template variable names (remove settlement and Dx suffix)
all_template_vars_clean <- unique(gsub("\\|Urban\\|Dx|\\|Rural\\|Dx", "",
                                       all_template_vars[str_detect(all_template_vars, "\\|Dx")]))

# Create plot data: one row per REGION x base-variable, avail = whether data exists
# Matches all three formats: |Urban|Dx, |Rural|Dx (template), |Urban|D1 (EUv2), |D1 (EU old)
plot_data <- data_input_format %>%
  filter(str_detect(VARIABLE, "\\|Dx$|\\|D[0-9]+$")) %>%
  mutate(VARIABLE_clean = gsub("\\|Urban\\|Dx$|\\|Rural\\|Dx$|\\|Urban\\|D[0-9]+$|\\|Rural\\|D[0-9]+$|\\|D[0-9]+$", "", VARIABLE)) %>%
  group_by(REGION, VARIABLE_clean) %>%
  summarize(avail = n(), .groups = "drop") %>%
  complete(REGION, VARIABLE_clean = all_template_vars_clean, fill = list(avail = 0))

ggplot(plot_data, aes(REGION, VARIABLE_clean, fill = factor(avail > 0))) +
  geom_tile() +
  theme_minimal() +
  theme(axis.text.x = element_text(angle=90, vjust = 0.5, hjust=1),
        axis.text.y = element_text(size=7)) +
  labs(x="Country", y="Variable") +
  scale_fill_manual(values = c("FALSE"="white", "TRUE"="steelblue"), name="Available") +
  guides(fill="none")
ggsave(path = folder, "Countries and Variables.png", width = 8, height=8)
#show expenditure shares - all curves per country, solid=urban, dashed=rural, colors=element
ggplot(data_output_allcountries %>%
         filter(element %in% c("energy_housing", "energy_transportation", "food") &
                var=="expcat_input" & !is.na(settlement) & !is.na(value)) %>%
         mutate(decile=as.numeric(gsub("D", "", dist))) %>%
         filter(!is.na(decile)) %>%
         arrange(iso3, settlement, element, decile)) +
  geom_line(aes(decile, value, color=element, linetype=settlement), linewidth=0.8) +
  facet_wrap(. ~ iso3, scales = "free_y", ncol=3) +
  theme_minimal() +
  theme(legend.position = "bottom") +
  labs(x="Decile", y="Expenditure share [%]",
       color="Expenditure type", linetype="Settlement") +
  scale_x_continuous(breaks=seq(1,10)) +
  scale_linetype_manual(values=c("rural"="dashed", "urban"="solid"))
ggsave(path = folder, "Energy Expenditure Shares.png", width = 12, height=10)
# Show expenditure deciles - separate by settlement (urban/rural)
# Expenditure is more robust than income (not affected by data reversal issues)
ggplot(data_output_allcountries %>%
         filter(var=="expenditure_decile" & !is.na(settlement) & !is.na(value)) %>%
         mutate(decile=as.numeric(gsub("D", "", dist))) %>%
         filter(!is.na(decile)) %>%
         arrange(iso3, settlement, decile)) +
  geom_line(aes(decile, value, color=settlement), linewidth=0.8) +
  geom_hline(yintercept=10, linetype="dashed", alpha=0.3, color="gray50") +
  facet_wrap(. ~ iso3, scales = "free_y") +
  theme_minimal() +
  theme(legend.position = "bottom") +
  labs(x="Decile (1=poorest, 10=richest)",
       y="Expenditure decile share [%]",
       title="Expenditure Distribution by Decile",
       subtitle="Lines should go UP from D1 to D10. Dashed line = perfect equality (10%)") +
  scale_x_continuous(breaks=seq(1,10))
ggsave(path = folder, "Expenditure deciles.png", width = 12, height=10)


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

  # 1. Expenditure distribution (more robust than income)
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


# Plot Theil indices - create faceted plot with 4 panels (one per variable)
theil_results_labeled <- theil_results %>%
  mutate(variable_label = case_when(
    variable == "income" ~ "Income",
    variable == "expenditure" ~ "Expenditure",
    variable == "energy_housing" ~ "Energy: Housing",
    variable == "energy_transportation" ~ "Energy: Transportation",
    TRUE ~ variable
  ))

ggplot(theil_results_labeled, aes(x=iso3, y=theil, fill=variable_label)) +
  geom_bar(stat="identity", show.legend=FALSE) +
  facet_wrap(~ variable_label, scales="fixed", ncol=2) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle=90, hjust=1, vjust=0.5, size=8),
        strip.text = element_text(size=11, face="bold")) +
  labs(x="Country", y="Theil T Index",
       title="Inequality Measures (Theil T Index)",
       subtitle="Fixed y-scale for comparability across variables") +
  scale_fill_brewer(palette="Set2")
ggsave(path = folder, "Theil_decomposition.png", width = 12, height=8)


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

  # For expenditure deciles (more robust than income)
  for(var_type in c("expenditure_decile")) {

    var_data <- country_data %>%
      filter(var == var_type) %>%
      mutate(decile_num = as.numeric(gsub("D", "", dist)),
             share = value / 100) %>%  # Share of total expenditure in this decile
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

    # Mean expenditure per capita in each group
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

    # Between-group Theil: Compare urban vs rural mean expenditures
    # Treating all urban as having urban mean, all rural as having rural mean
    group_income_shares <- c(urban_total_income_share, rural_total_income_share)
    group_pop_weights <- c(urban_pop_share, rural_pop_share)
    between_theil <- calculate_theil(group_income_shares, group_pop_weights)

    # Within-group Theil: Inequality within urban + within rural
    # For urban: each urban decile has 10% of urban population
    urban_theil <- calculate_theil(urban_data$share / sum(urban_data$share))
    rural_theil <- calculate_theil(rural_data$share / sum(rural_data$share))

    # Weight by group's expenditure share (not population share)
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


# ============================================================================
# ADDITIONAL COMPREHENSIVE VISUALIZATIONS
# ============================================================================

cat("\n\n=== Creating Additional Visualizations ===\n\n")

# Prepare comparison data for plotting
comparison_data <- data.frame()
for(country in unique(data_output_allcountries$iso3)) {
  country_data <- data_output_allcountries %>% filter(iso3 == country & !is.na(settlement))

  for(var_type in c("expenditure_decile")) {
    for(settlement_type in c("urban", "rural")) {
      var_data <- country_data %>%
        filter(var == var_type, settlement == settlement_type) %>%
        mutate(decile_num = as.numeric(gsub("D", "", dist)),
               share = value / 100) %>%
        filter(!is.na(share) & !is.na(decile_num))

      if(nrow(var_data) >= 10) {
        shares <- var_data$share / sum(var_data$share)
        theil <- calculate_theil(shares)
        comparison_data <- rbind(comparison_data, data.frame(
          iso3 = country, variable = var_type, settlement = settlement_type, theil = theil
        ))
      }
    }
  }

  for(energy_type in c("energy_housing", "energy_transportation")) {
    for(settlement_type in c("urban", "rural")) {
      energy_data <- country_data %>%
        filter(var == "expcat_input", element == energy_type, settlement == settlement_type) %>%
        mutate(decile_num = as.numeric(gsub("D", "", dist)),
               share = value / 100) %>%
        filter(!is.na(share) & !is.na(decile_num))

      if(nrow(energy_data) >= 10) {
        shares <- energy_data$share / sum(energy_data$share)
        theil <- calculate_theil(shares)
        comparison_data <- rbind(comparison_data, data.frame(
          iso3 = country, variable = energy_type, settlement = settlement_type, theil = theil
        ))
      }
    }
  }
}

comparison_wide <- comparison_data %>%
  pivot_wider(names_from = settlement, values_from = theil) %>%
  mutate(urban_rural_ratio = urban / rural,
         difference = urban - rural)

# 1. Scatter Plot - All Variables
cat("  Creating scatter plot - all variables...\n")
scatter_all <- comparison_wide %>%
  mutate(variable_label = case_when(
    variable == "expenditure_decile" ~ "Expenditure",
    variable == "energy_housing" ~ "Energy: Housing",
    variable == "energy_transportation" ~ "Energy: Transport",
    TRUE ~ variable
  ))

ggplot(scatter_all, aes(x = rural, y = urban, color = variable_label, shape = variable_label)) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "gray40", linewidth = 1) +
  geom_point(size = 4, alpha = 0.8) +
  geom_text(data = scatter_all %>% filter(abs(urban - rural) > 0.15 | urban > 0.4),
            aes(label = iso3), size = 3, hjust = -0.2, vjust = -0.2, show.legend = FALSE) +
  scale_color_brewer(palette = "Set2", name = "Variable") +
  scale_shape_manual(values = c(16, 17, 15), name = "Variable") +
  theme_minimal(base_size = 12) +
  theme(legend.position = "bottom") +
  labs(x = "Rural Inequality (Theil Index)",
       y = "Urban Inequality (Theil Index)",
       title = "Urban vs Rural Inequality: All Variables") +
  coord_fixed()

# 2. Scatter Plot - Expenditure Only (Zoomed)
cat("  Creating scatter plot - expenditure categories only...\n")
scatter_exp <- comparison_wide %>%
  mutate(variable_label = case_when(
    variable == "expenditure_decile" ~ "Total Expenditure",
    variable == "energy_housing" ~ "Housing Energy",
    variable == "energy_transportation" ~ "Transport Energy",
    TRUE ~ variable
  ))

ggplot(scatter_exp, aes(x = rural, y = urban, color = variable_label, shape = variable_label)) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "gray40", linewidth = 1) +
  geom_point(size = 4, alpha = 0.8) +
  geom_text(data = scatter_exp %>%
              filter(variable == "expenditure_decile" & (abs(urban - rural) > 0.05 | urban > 0.15)),
            aes(label = iso3), size = 3, hjust = -0.2, vjust = -0.2, show.legend = FALSE) +
  scale_color_manual(values = c("Total Expenditure" = "#E69F00",
                                  "Housing Energy" = "#56B4E9",
                                  "Transport Energy" = "#009E73"),
                     name = "Category") +
  scale_shape_manual(values = c(16, 17, 15), name = "Category") +
  theme_minimal(base_size = 12) +
  theme(legend.position = "bottom") +
  labs(x = "Rural Inequality (Theil Index)",
       y = "Urban Inequality (Theil Index)",
       title = "Urban vs Rural Inequality: Expenditure Categories (Zoomed)",
       subtitle = "Better scale for comparing expenditure patterns") +
  coord_fixed(xlim = c(0, 0.35), ylim = c(0, 0.35))

# 3. Scatter Plot - Energy Expenditures ONLY
cat("  Creating scatter plot - energy expenditures only...\n")
scatter_energy <- comparison_wide %>%
  filter(variable %in% c("energy_housing", "energy_transportation")) %>%
  mutate(variable_label = case_when(
    variable == "energy_housing" ~ "Housing Energy",
    variable == "energy_transportation" ~ "Transport Energy",
    TRUE ~ variable
  ))

ggplot(scatter_energy, aes(x = rural, y = urban, color = variable_label, shape = variable_label)) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "gray40", linewidth = 1) +
  geom_point(size = 4, alpha = 0.8) +
  geom_text(data = scatter_energy %>% filter(abs(urban - rural) > 0.02 | urban > 0.08),
            aes(label = iso3), size = 3, hjust = -0.2, vjust = -0.2, show.legend = FALSE) +
  scale_color_manual(values = c("Housing Energy" = "#56B4E9",
                                  "Transport Energy" = "#009E73"),
                     name = "Energy Type") +
  scale_shape_manual(values = c(17, 15), name = "Energy Type") +
  theme_minimal(base_size = 12) +
  theme(legend.position = "bottom") +
  labs(x = "Rural Inequality (Theil Index)",
       y = "Urban Inequality (Theil Index)",
       title = "Urban vs Rural Inequality: Energy Expenditures Only",
       subtitle = "Housing vs Transportation energy inequality patterns") +
  coord_fixed()

# 4. Urban-Rural Gap Ranking
cat("  Creating urban-rural gap ranking...\n")
gap_data <- comparison_wide %>%
  filter(variable == "expenditure_decile") %>%
  arrange(desc(difference)) %>%
  mutate(iso3 = factor(iso3, levels = iso3),
         gap_direction = ifelse(difference > 0, "Urban Higher", "Rural Higher"))

ggplot(gap_data, aes(x = iso3, y = difference, fill = gap_direction)) +
  geom_col() +
  geom_hline(yintercept = 0, color = "black", linewidth = 0.5) +
  scale_fill_manual(values = c("Urban Higher" = "#56B4E9", "Rural Higher" = "#E69F00")) +
  theme_minimal(base_size = 11) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1, vjust = 0.5, size = 9)) +
  labs(x = "Country", y = "Urban - Rural Gap (Theil)",
       title = "Urban-Rural Inequality Gap: Expenditure",
       fill = "Higher Inequality")

# 5. Top/Bottom Decile Comparison
cat("  Creating top/bottom decile comparison...\n")
decile_extremes <- data_output_allcountries %>%
  filter(var == "expenditure_decile", !is.na(settlement)) %>%
  mutate(decile = as.numeric(gsub("D", "", dist))) %>%
  filter(decile %in% c(1, 10)) %>%
  select(iso3, settlement, decile, value) %>%
  pivot_wider(names_from = decile, values_from = value, names_prefix = "D")

ggplot(decile_extremes, aes(x = D1, y = D10, color = settlement, shape = settlement)) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "gray40") +
  geom_point(size = 4, alpha = 0.8) +
  geom_text(data = decile_extremes %>% filter(D10 > 25 | D1 < 4),
            aes(label = iso3), size = 3, hjust = -0.2, vjust = -0.2, show.legend = FALSE) +
  scale_color_manual(values = c("urban" = "#56B4E9", "rural" = "#E69F00")) +
  scale_shape_manual(values = c(16, 17)) +
  theme_minimal(base_size = 12) +
  labs(x = "Poorest Decile Share (%)", y = "Richest Decile Share (%)",
       title = "Expenditure: Poorest vs Richest Decile",
       subtitle = "Distance from diagonal = degree of inequality",
       color = "Settlement", shape = "Settlement")

# 6. Lorenz Curves for Selected Countries
cat("  Creating Lorenz curves...\n")
selected_countries <- c("ZAF", "USA", "CHN", "DNK", "IND", "DEU")
lorenz_data <- data_output_allcountries %>%
  filter(var == "expenditure_decile", iso3 %in% selected_countries, !is.na(settlement)) %>%
  mutate(decile = as.numeric(gsub("D", "", dist))) %>%
  filter(!is.na(decile)) %>%
  arrange(iso3, settlement, decile) %>%
  group_by(iso3, settlement) %>%
  mutate(cum_pop = decile * 10,
         cum_expenditure = cumsum(value)) %>%
  ungroup() %>%
  bind_rows(data.frame(
    iso3 = rep(selected_countries, each = 2),
    settlement = rep(c("urban", "rural"), length(selected_countries)),
    decile = 0, cum_pop = 0, cum_expenditure = 0
  )) %>%
  arrange(iso3, settlement, decile)

ggplot(lorenz_data, aes(x = cum_pop, y = cum_expenditure, color = settlement)) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "gray40") +
  geom_line(linewidth = 1) +
  geom_point(data = lorenz_data %>% filter(decile > 0), size = 2) +
  facet_wrap(~ iso3, ncol = 3) +
  scale_color_manual(values = c("urban" = "#56B4E9", "rural" = "#E69F00")) +
  theme_minimal(base_size = 11) +
  labs(x = "Cumulative Population (%)", y = "Cumulative Expenditure (%)",
       title = "Lorenz Curves: Expenditure Distribution",
       color = "Settlement") +
  coord_fixed()

# 7. Concentration Ratios
cat("  Creating concentration ratio plot...\n")
concentration <- data_output_allcountries %>%
  filter(var == "expenditure_decile", !is.na(settlement)) %>%
  mutate(decile = as.numeric(gsub("D", "", dist))) %>%
  filter(decile %in% c(1, 10)) %>%
  select(iso3, settlement, decile, value) %>%
  pivot_wider(names_from = decile, values_from = value, names_prefix = "D") %>%
  mutate(concentration_ratio = D10 / D1) %>%
  arrange(desc(concentration_ratio))

ggplot(concentration, aes(x = reorder(paste(iso3, settlement), concentration_ratio),
                          y = concentration_ratio, fill = settlement)) +
  geom_col() +
  geom_hline(yintercept = 1, linetype = "dashed", color = "red") +
  scale_fill_manual(values = c("urban" = "#56B4E9", "rural" = "#E69F00")) +
  theme_minimal(base_size = 10) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1, vjust = 0.5, size = 7)) +
  labs(x = "Country-Settlement", y = "Concentration Ratio (D10/D1)",
       title = "Expenditure Concentration: Richest/Poorest Ratio",
       fill = "Settlement")

cat("\n✓ All visualizations added to PDF\n")


# ============================================================================
# 8. WORLD MAP: Gini Index of Total Expenditures
# ============================================================================

cat("  Creating world map with Gini index...\n")

# Gini from decile shares: Gini = (2 * sum(i * s_i)) / (n * sum(s_i)) - (n+1)/n
# where s_i are shares sorted ascending (D1=poorest to D10=richest)
calculate_gini <- function(shares) {
  shares <- shares[!is.na(shares) & shares > 0]
  n <- length(shares)
  if(n < 2) return(NA)
  s <- sort(shares)          # ascending: poorest first
  s <- s / sum(s)            # normalize to sum to 1
  gini <- (2 * sum((1:n) * s)) / (n * sum(s)) - (n + 1) / n
  return(gini)
}

# Calculate Gini per country-settlement, then average across settlements
gini_data <- data_output_allcountries %>%
  filter(var == "expenditure_decile", !is.na(value), !is.na(settlement)) %>%
  mutate(decile = as.numeric(gsub("D", "", dist))) %>%
  filter(!is.na(decile)) %>%
  arrange(iso3, settlement, decile) %>%
  group_by(iso3, settlement) %>%
  summarize(
    gini     = calculate_gini(value),
    n_deciles = n(),
    .groups  = "drop"
  ) %>%
  filter(!is.na(gini), n_deciles >= 10) %>%
  group_by(iso3) %>%
  summarize(gini = mean(gini, na.rm = TRUE), .groups = "drop")

cat("Gini indices by country:\n")
print(gini_data %>% arrange(desc(gini)))

library(maps)
world_map <- map_data("world")
world_map$iso3 <- countrycode(world_map$region, origin = "country.name",
                               destination = "iso3c", warn = FALSE)
world_map <- world_map %>% left_join(gini_data, by = "iso3")

map_plot <- ggplot(world_map, aes(x = long, y = lat, group = group, fill = gini)) +
  geom_polygon(color = "white", linewidth = 0.15) +
  scale_fill_viridis_c(
    name      = "Gini\n(Expenditure)",
    na.value  = "grey70",
    option    = "plasma",
    direction = -1,
    labels    = scales::number_format(accuracy = 0.01)
  ) +
  coord_fixed(1.3, xlim = c(-180, 180), ylim = c(-60, 85)) +
  theme_minimal(base_size = 12) +
  theme(
    panel.grid      = element_blank(),
    axis.text       = element_blank(),
    axis.ticks      = element_blank(),
    axis.title      = element_blank(),
    legend.position = "right",
    plot.title      = element_text(face = "bold")
  ) +
  labs(
    title    = "Gini Index of Total Expenditure Inequality",
    subtitle = "Averaged across urban and rural settlements  |  Grey = data not available",
    caption  = "Source: NAVIGATE Inequality Dataset"
  )

print(map_plot)
ggsave(file.path(folder, "gini_world_map.png"), plot = map_plot, width = 14, height = 8, dpi = 150)
cat("  Gini world map saved to settlement/gini_world_map.png\n")


dev.off()
