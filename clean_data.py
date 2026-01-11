import pandas as pd
import os
from pathlib import Path


def read_betfair_bz2(file_path):
    try:
        df = pd.read_json(
            file_path, compression="bz2", lines=True, dtype={"clk": str, "pt": "Int64"}
        )
        return df
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return pd.DataFrame()


def parse_betfair_stream(df):
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    market_records = []
    runner_static_records = []
    runner_change_records = []

    for row in df.itertuples():
        if not hasattr(row, "mc"):
            continue

        try:
            if pd.isna(row.mc):
                continue
        except (ValueError, TypeError):
            pass

        mc_data = row.mc if isinstance(row.mc, list) else [row.mc]
        if not mc_data:
            continue

        publish_time = pd.to_datetime(row.pt, unit="ms") if hasattr(row, "pt") else None

        for market in mc_data:
            market_id = market.get("id")

            if "marketDefinition" in market:
                mkt_def = market["marketDefinition"]

                market_records.append(
                    {
                        "market_id": market_id,
                        "publish_time": publish_time,
                        "status": mkt_def.get("status"),
                        "in_play": mkt_def.get("inPlay"),
                        "number_of_active_runners": mkt_def.get(
                            "numberOfActiveRunners"
                        ),
                        "total_matched": mkt_def.get("totalMatched"),
                        "version": mkt_def.get("version"),
                    }
                )

                if "runners" in mkt_def:
                    for runner in mkt_def["runners"]:
                        runner_static_records.append(
                            {
                                "market_id": market_id,
                                "publish_time": publish_time,
                                "runner_id": runner.get("id"),
                                "runner_name": runner.get("name"),
                                "status": runner.get("status"),
                                "removal_date": (
                                    pd.to_datetime(runner.get("removalDate"))
                                    if runner.get("removalDate")
                                    else None
                                ),
                            }
                        )

            if "rc" in market:
                for rc in market["rc"]:
                    runner_change_records.append(
                        {
                            "market_id": market_id,
                            "publish_time": publish_time,
                            "runner_id": rc.get("id"),
                            "ltp": rc.get("ltp"),
                            "tv": rc.get("tv"),
                            "batb": rc.get("batb"),
                            "batl": rc.get("batl"),
                            "spn": rc.get("spn"),
                            "spf": rc.get("spf"),
                        }
                    )

    return (
        pd.DataFrame(market_records),
        pd.DataFrame(runner_static_records),
        pd.DataFrame(runner_change_records),
    )


def create_reference_tables(df_list):
    market_metadata = []
    runner_metadata = []

    for df in df_list:
        if df.empty:
            continue

        for row in df.itertuples():
            if not hasattr(row, "mc"):
                continue

            try:
                if pd.isna(row.mc):
                    continue
            except (ValueError, TypeError):
                pass

            mc_data = row.mc if isinstance(row.mc, list) else [row.mc]
            if not mc_data:
                continue

            for market in mc_data:
                if "marketDefinition" not in market:
                    continue

                mkt_def = market["marketDefinition"]
                market_id = market.get("id")

                market_metadata.append(
                    {
                        "market_id": market_id,
                        "event_id": mkt_def.get("eventId"),
                        "event_name": mkt_def.get("eventName"),
                        "event_type_id": mkt_def.get("eventTypeId"),
                        "market_name": mkt_def.get("name"),
                        "market_type": mkt_def.get("marketType"),
                        "market_time": (
                            pd.to_datetime(mkt_def.get("marketTime"))
                            if mkt_def.get("marketTime")
                            else None
                        ),
                        "venue": mkt_def.get("venue"),
                        "country_code": mkt_def.get("countryCode"),
                        "number_of_winners": mkt_def.get("numberOfWinners"),
                    }
                )

                if "runners" in mkt_def:
                    for runner in mkt_def["runners"]:
                        runner_metadata.append(
                            {
                                "market_id": market_id,
                                "runner_id": runner.get("id"),
                                "runner_name": runner.get("name"),
                            }
                        )

    market_ref = (
        pd.DataFrame(market_metadata).drop_duplicates(subset=["market_id"])
        if market_metadata
        else pd.DataFrame()
    )
    runner_ref = (
        pd.DataFrame(runner_metadata).drop_duplicates(subset=["market_id", "runner_id"])
        if runner_metadata
        else pd.DataFrame()
    )

    return market_ref, runner_ref


def get_folder_size(folder_path):
    total_size = 0
    for dirpath, _, filenames in os.walk(folder_path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            if not os.path.islink(filepath):
                total_size += os.path.getsize(filepath)

    size_mb = total_size / (1024 * 1024)
    print(f"Total size of '{folder_path}': {size_mb:.2f} MB")


def process_month(base_path, output_dir, year, month):
    """
    Process Betfair data for a given month.
    Parameters:
    - base_path: str or Path, base directory containing raw data
    - output_dir: str or Path, directory to save processed data
    - year: int, year to process (e.g., 2024)
    - month: str, month to process (e.g., 'Dec')
    Returns:
    - dict with summary of processed data
    - saves processed files to output_dir
    """
    base_path = Path(base_path)
    month_path = base_path / str(year) / month

    if not month_path.exists():
        return

    output_path = Path(output_dir) / f"{year}_{month}"
    output_path.mkdir(parents=True, exist_ok=True)

    days = sorted(
        [d for d in month_path.iterdir() if d.is_dir()], key=lambda d: int(d.name)
    )
    print(f"Processing {year}/{month} - found {len(days)} days")

    all_markets = []
    all_runner_static = []
    all_raw_dfs = []

    for day_path in days:
        day = day_path.name

        bz2_files = list(day_path.rglob("*.bz2"))

        day_runner_changes = []

        batch_size = 100
        for i in range(0, len(bz2_files), batch_size):
            batch = bz2_files[i : i + batch_size]

            for file_path in batch:
                try:
                    df = read_betfair_bz2(file_path)
                    if df.empty:
                        continue

                    all_raw_dfs.append(df)
                    m, rs, rc = parse_betfair_stream(df)

                    if not m.empty:
                        all_markets.append(m)
                    if not rs.empty:
                        all_runner_static.append(rs)
                    if not rc.empty:
                        day_runner_changes.append(rc)

                except Exception as e:
                    print(f"    Error processing {file_path.name}: {e}")
                    continue

        if day_runner_changes:
            daily_prices = pd.concat(day_runner_changes, ignore_index=True)
            daily_prices = daily_prices.sort_values(["market_id", "publish_time"])

            daily_file = (
                output_path
                / "daily_prices"
                / f"runner_prices_{year}_{month}_{day}.parquet"
            )
            daily_file.parent.mkdir(exist_ok=True)
            daily_prices.to_parquet(daily_file, index=False)

    print("\n  Creating monthly reference tables...")
    market_ref, runner_ref = create_reference_tables(all_raw_dfs)

    print("  Combining monthly aggregates...")
    combined_markets = (
        pd.concat(all_markets, ignore_index=True) if all_markets else pd.DataFrame()
    )
    combined_runner_static = (
        pd.concat(all_runner_static, ignore_index=True)
        if all_runner_static
        else pd.DataFrame()
    )

    if not combined_markets.empty:
        combined_markets = combined_markets.drop_duplicates(
            subset=["market_id", "status", "in_play", "version"], keep="first"
        ).sort_values(["market_id", "publish_time"])

    if not combined_runner_static.empty:
        combined_runner_static = combined_runner_static.drop_duplicates(
            subset=["market_id", "runner_id", "status"], keep="first"
        ).sort_values(["market_id", "publish_time"])

    if not market_ref.empty:
        market_ref.to_parquet(output_path / "market_reference.parquet", index=False)

    if not runner_ref.empty:
        runner_ref.to_parquet(output_path / "runner_reference.parquet", index=False)

    if not combined_markets.empty:
        combined_markets.to_parquet(
            output_path / "market_timeline.parquet", index=False
        )

    if not combined_runner_static.empty:
        combined_runner_static.to_parquet(
            output_path / "runner_status.parquet", index=False
        )

    daily_files = list((output_path / "daily_prices").glob("*.parquet"))

    print(f"\nProcessing complete for {year}/{month}:")
    get_folder_size(output_path)

    return {
        "year": year,
        "month": month,
        "num_markets": len(market_ref) if not market_ref.empty else 0,
        "num_runners": len(runner_ref) if not runner_ref.empty else 0,
        "num_daily_files": len(daily_files),
    }


if __name__ == "__main__":
    process_month("data/raw_data", year=2024, month="Dec", output_dir="data/proc_data")

    # Output structure will be:
    # processed/
    #   2024_Dec/
    #     market_reference.parquet          # Monthly
    #     runner_reference.parquet          # Monthly
    #     market_timeline.parquet           # Monthly
    #     runner_status.parquet             # Monthly
    #     daily_prices/
    #       runner_prices_2024_Dec_1.parquet   # Daily
    #       runner_prices_2024_Dec_2.parquet   # Daily
    #       runner_prices_2024_Dec_3.parquet   # Daily
    #       ...
