"""
Validate output from data processing skills.

Usage:
    python execution/validate_skill_output.py decision-maker-identifier <file_path>
    python execution/validate_skill_output.py company-name-normalizer <file_path>
    python execution/validate_skill_output.py lead-niche-categorizer <file_path> --categories "Cat1|Cat2|Cat3"
    python execution/validate_skill_output.py social-media-content-generator <week_folder>
"""

import sys
import argparse
from pathlib import Path
import pandas as pd


class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


def check(condition, message):
    """Print check result with color coding."""
    if condition:
        print(f"{Colors.GREEN}✓{Colors.RESET} {message}")
        return True
    else:
        print(f"{Colors.RED}✗{Colors.RESET} {message}")
        return False


def validate_decision_maker_identifier(file_path):
    """Validate Decision Maker Identifier output."""
    print(f"\n{Colors.BOLD}VALIDATING: Decision Maker Identifier{Colors.RESET}")
    print(f"File: {file_path}\n")

    file_path = Path(file_path)
    all_checks = []

    # Check file exists
    all_checks.append(check(file_path.exists(), f"File exists: {file_path.name}"))

    if not file_path.exists():
        return False

    # Check backup exists
    backup_path = file_path.parent / f"{file_path.stem}_backup{file_path.suffix}"
    all_checks.append(check(backup_path.exists(), f"Backup created: {backup_path.name}"))

    # Check decision makers CSV exists
    dm_csv_path = file_path.parent / f"{file_path.stem}_DECISION_MAKERS.csv"
    all_checks.append(check(dm_csv_path.exists(), f"Decision makers CSV created: {dm_csv_path.name}"))

    # Read and validate Excel
    try:
        df = pd.read_excel(file_path)

        # Check Decision_Maker column exists
        has_dm_col = 'Decision_Maker' in df.columns
        all_checks.append(check(has_dm_col, "Has 'Decision_Maker' column"))

        # Check Confidence column exists
        has_conf_col = 'Confidence' in df.columns
        all_checks.append(check(has_conf_col, "Has 'Confidence' column"))

        if has_dm_col:
            # Validate Decision_Maker values
            valid_dm_values = set(['Yes', 'No'])
            actual_dm_values = set(df['Decision_Maker'].dropna().unique())
            dm_values_valid = actual_dm_values.issubset(valid_dm_values)
            all_checks.append(check(dm_values_valid, f"Decision_Maker values are valid (Yes/No only): {actual_dm_values}"))

        if has_conf_col:
            # Validate Confidence values
            valid_conf_values = set(['High', 'Medium', 'Low'])
            actual_conf_values = set(df['Confidence'].dropna().unique())
            conf_values_valid = actual_conf_values.issubset(valid_conf_values)
            all_checks.append(check(conf_values_valid, f"Confidence values are valid (High/Medium/Low only): {actual_conf_values}"))

        # Check distribution
        if has_dm_col:
            yes_count = (df['Decision_Maker'] == 'Yes').sum()
            no_count = (df['Decision_Maker'] == 'No').sum()
            total = len(df)
            print(f"\n{Colors.BOLD}Distribution:{Colors.RESET}")
            print(f"  Yes: {yes_count} ({yes_count/total*100:.1f}%)")
            print(f"  No: {no_count} ({no_count/total*100:.1f}%)")
            print(f"  Total: {total}")

    except Exception as e:
        all_checks.append(check(False, f"Error reading Excel: {str(e)}"))

    # Summary
    print(f"\n{Colors.BOLD}Summary:{Colors.RESET} {sum(all_checks)}/{len(all_checks)} checks passed")
    return all(all_checks)


def validate_company_name_normalizer(file_path):
    """Validate Company Name Normalizer output."""
    print(f"\n{Colors.BOLD}VALIDATING: Company Name Normalizer{Colors.RESET}")
    print(f"File: {file_path}\n")

    file_path = Path(file_path)
    all_checks = []

    # Check file exists
    all_checks.append(check(file_path.exists(), f"File exists: {file_path.name}"))

    if not file_path.exists():
        return False

    # Check backup exists
    backup_path = file_path.parent / f"{file_path.stem}_backup{file_path.suffix}"
    all_checks.append(check(backup_path.exists(), f"Backup created: {backup_path.name}"))

    # Read and validate Excel
    try:
        df = pd.read_excel(file_path)

        # Check Clean_Company_Name column exists
        has_clean_col = 'Clean_Company_Name' in df.columns
        all_checks.append(check(has_clean_col, "Has 'Clean_Company_Name' column"))

        if has_clean_col:
            # Check for legal suffixes that should be removed
            clean_names = df['Clean_Company_Name'].dropna().astype(str)
            legal_suffixes = ['Inc', 'LLC', 'Corp', 'Ltd', 'Limited', 'Corporation', 'Incorporated']

            has_suffixes = clean_names.str.contains('|'.join(legal_suffixes), case=False, regex=True).any()
            all_checks.append(check(not has_suffixes, "Clean names don't contain legal suffixes (Inc, LLC, Corp, Ltd)"))

            # Show sample transformations
            if 'Company' in df.columns or 'Company Name' in df.columns:
                company_col = 'Company' if 'Company' in df.columns else 'Company Name'
                print(f"\n{Colors.BOLD}Sample Transformations:{Colors.RESET}")
                samples = df[[company_col, 'Clean_Company_Name']].head(10)
                for idx, row in samples.iterrows():
                    orig = row[company_col]
                    clean = row['Clean_Company_Name']
                    if pd.notna(orig) and pd.notna(clean) and str(orig) != str(clean):
                        print(f"  {orig} → {clean}")

    except Exception as e:
        all_checks.append(check(False, f"Error reading Excel: {str(e)}"))

    # Summary
    print(f"\n{Colors.BOLD}Summary:{Colors.RESET} {sum(all_checks)}/{len(all_checks)} checks passed")
    return all(all_checks)


def validate_lead_niche_categorizer(file_path, categories):
    """Validate Lead Niche Categorizer output."""
    print(f"\n{Colors.BOLD}VALIDATING: Lead Niche Categorizer{Colors.RESET}")
    print(f"File: {file_path}")
    print(f"Expected categories: {categories}\n")

    file_path = Path(file_path)
    all_checks = []

    # Check file exists
    all_checks.append(check(file_path.exists(), f"File exists: {file_path.name}"))

    if not file_path.exists():
        return False

    # Check backup exists
    backup_path = file_path.parent / f"{file_path.stem}_backup{file_path.suffix}"
    all_checks.append(check(backup_path.exists(), f"Backup created: {backup_path.name}"))

    # Check invalid data file exists (if any failed)
    invalid_path = file_path.parent / f"{file_path.stem}_INVALID{file_path.suffix}"

    # Check by_niche folder exists
    niche_folder = file_path.parent / f"{file_path.stem}_by_niche"
    all_checks.append(check(niche_folder.exists(), f"Niche folder created: {niche_folder.name}"))

    # Read and validate Excel
    try:
        df = pd.read_excel(file_path)

        # Check Verified_Niche column exists
        has_niche_col = 'Verified_Niche' in df.columns
        all_checks.append(check(has_niche_col, "Has 'Verified_Niche' column"))

        if has_niche_col and categories:
            # Parse categories
            valid_categories = set(cat.strip() for cat in categories.split('|'))
            valid_categories.add('Categorization Failed')
            valid_categories.add('Insufficient Data')

            # Validate category values
            actual_categories = set(df['Verified_Niche'].dropna().unique())
            invalid_categories = actual_categories - valid_categories

            categories_valid = len(invalid_categories) == 0
            if not categories_valid:
                all_checks.append(check(False, f"Found invalid categories: {invalid_categories}"))
            else:
                all_checks.append(check(True, f"All categories are valid"))

            # Check distribution
            print(f"\n{Colors.BOLD}Category Distribution:{Colors.RESET}")
            category_counts = df['Verified_Niche'].value_counts()
            total = len(df)
            for cat, count in category_counts.items():
                print(f"  {cat}: {count} ({count/total*100:.1f}%)")

            # Check CSV files
            if niche_folder.exists():
                print(f"\n{Colors.BOLD}CSV Files:{Colors.RESET}")
                csv_files = list(niche_folder.glob("*.csv"))
                for csv_file in csv_files:
                    print(f"  {csv_file.name}")

    except Exception as e:
        all_checks.append(check(False, f"Error reading Excel: {str(e)}"))

    # Check if invalid file exists
    if invalid_path.exists():
        print(f"\n{Colors.YELLOW}⚠{Colors.RESET} Invalid data file exists: {invalid_path.name}")
        try:
            invalid_df = pd.read_excel(invalid_path)
            print(f"  Rows in invalid file: {len(invalid_df)}")
        except:
            pass

    # Summary
    print(f"\n{Colors.BOLD}Summary:{Colors.RESET} {sum(all_checks)}/{len(all_checks)} checks passed")
    return all(all_checks)


def validate_social_media_content_generator(week_folder):
    """Validate Social Media Content Generator output."""
    print(f"\n{Colors.BOLD}VALIDATING: Social Media Content Generator{Colors.RESET}")
    print(f"Week folder: {week_folder}\n")

    week_folder = Path(week_folder)
    all_checks = []

    # Check folder exists
    all_checks.append(check(week_folder.exists(), f"Week folder exists: {week_folder.name}"))

    if not week_folder.exists():
        return False

    # Check required files
    required_files = [
        'CONTENT_CALENDAR.md',
        'LINKEDIN_POSTS.md',
        'X_POSTS.md',
        'RESEARCH_REPORT.md',
        'HASHTAGS.md'
    ]

    for file_name in required_files:
        file_path = week_folder / file_name
        all_checks.append(check(file_path.exists(), f"Has {file_name}"))

    # Check LinkedIn posts count
    linkedin_path = week_folder / 'LINKEDIN_POSTS.md'
    if linkedin_path.exists():
        try:
            content = linkedin_path.read_text(encoding='utf-8')
            # Count posts by looking for post markers
            post_count = content.count('## Post')
            expected_linkedin = 5
            all_checks.append(check(post_count == expected_linkedin, f"LinkedIn posts count: {post_count}/{expected_linkedin}"))
        except Exception as e:
            all_checks.append(check(False, f"Error reading LinkedIn posts: {str(e)}"))

    # Check X posts count
    x_path = week_folder / 'X_POSTS.md'
    if x_path.exists():
        try:
            content = x_path.read_text(encoding='utf-8')
            # Count posts by looking for post markers
            post_count = content.count('## Post') or content.count('### Post')
            expected_x = 15
            all_checks.append(check(post_count == expected_x, f"X/Twitter posts count: {post_count}/{expected_x}"))
        except Exception as e:
            all_checks.append(check(False, f"Error reading X posts: {str(e)}"))

    # Check images folder
    images_folder = week_folder / 'images'
    has_images = images_folder.exists()
    if has_images:
        image_files = list(images_folder.glob("*.png"))
        print(f"\n{Colors.BOLD}Images:{Colors.RESET}")
        print(f"  Found {len(image_files)} image(s)")
        for img in image_files[:5]:  # Show first 5
            print(f"  - {img.name}")

    # Summary
    print(f"\n{Colors.BOLD}Summary:{Colors.RESET} {sum(all_checks)}/{len(all_checks)} checks passed")
    return all(all_checks)


def main():
    parser = argparse.ArgumentParser(description='Validate skill output')
    parser.add_argument('skill', choices=[
        'decision-maker-identifier',
        'company-name-normalizer',
        'lead-niche-categorizer',
        'social-media-content-generator'
    ])
    parser.add_argument('path', help='Path to file or folder')
    parser.add_argument('--categories', help='Expected categories (for lead-niche-categorizer)')

    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"{Colors.BOLD}SKILL OUTPUT VALIDATION{Colors.RESET}")
    print(f"{'='*60}")

    if args.skill == 'decision-maker-identifier':
        success = validate_decision_maker_identifier(args.path)
    elif args.skill == 'company-name-normalizer':
        success = validate_company_name_normalizer(args.path)
    elif args.skill == 'lead-niche-categorizer':
        if not args.categories:
            print(f"\n{Colors.RED}ERROR:{Colors.RESET} --categories required for lead-niche-categorizer")
            return 1
        success = validate_lead_niche_categorizer(args.path, args.categories)
    elif args.skill == 'social-media-content-generator':
        success = validate_social_media_content_generator(args.path)

    print(f"\n{'='*60}")
    if success:
        print(f"{Colors.GREEN}{Colors.BOLD}✓ VALIDATION PASSED{Colors.RESET}")
    else:
        print(f"{Colors.RED}{Colors.BOLD}✗ VALIDATION FAILED{Colors.RESET}")
    print(f"{'='*60}\n")

    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
