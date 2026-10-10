#used for cli command for docker containers 
#also takes the paysim data and adds the fraud patterns and publishes them to kafka

from __future__ import annotations
from dataclasses import replace
from datetime import datetime,timezone
import argparse
import logging
from pathlib import Path
from generator.config import GeneratorConfig
from generator.producer import  replay
import sys
from collections.abc import Sequence #used in sequence execution

logger = logging.getLogger("generator")

EXIT_OK = 0
EXIT_REPLAY_FAILED = 1
EXIT_INVALID_CONFIG = 2
EXIT_INTERRUPTED = 130

#used to chage date due to ternimal issue

def _parse_start(raw:str)-> datetime:
    parsed = datetime.fromisoformat(raw)
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo  is None else parsed

#check the number of processed events 
def _positive_int(raw:str)-> int:
    value = int(raw)
    if value <=0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return value

#used to build the command line argument parser
def _build_parser()-> argparse.ArgumentParser:

    parser = argparse.ArgumentParser(
        prog= "python -m generator.main",#
        description= "Replay Paysim with injected fraud scenario into kafka"
    ) 
    parser.add_argument("--csv-path",type = Path,help="Paysim CSV to replay")
    parser.add_argument("--bootstrap-servers",help="comma-separated kafka bootstrap severs")
    parser.add_argument("--topic",help="kafka topic to publish to")
    parser.add_argument("--simulation-start",type=_parse_start,help="ISO-8601 instant that paysim step 1 maps to",)
    parser.add_argument("--speedup",type=float,help="simulated seconds per wall-clock second (3600 = one step per second)",)
    parser.add_argument(
        "--ground-truth-path", type=Path, help="output file for the label log",)
    parser.add_argument("--seed", type=int, help="seed for injected scenarios")
    parser.add_argument("--structuring-scenarios", type=int, help="number of structuring scenarios")
    parser.add_argument(
        "--geo-anomaly-scenarios", type=int, help="number of geo-anomaly scenarios", )
    parser.add_argument("--max-events",type=_positive_int, help="stop after this many events, for smoke tests", )
    parser.add_argument("--log-level",default="INFO",choices=["DEBUG", "INFO", "WARNING", "ERROR"],help="logging verbosity (default: INFO)",)
    return parser

#used for loading standard configuration default environment varribales
def _reslove_settings (args: argparse.Namespace)-> GeneratorConfig:
    #argnparse.Namespace will hold the parsed values provided
    override = {
        "csv_path": args.csv_path,
        "kafka_bootstrap_servers": args.bootstrap_servers,
        "kafka_topic":args.topic,
        "simulation_start":args.simulation_start,
        "replay_speedup":args.speedup,
        "ground_truth_path":args.ground_truth_path,
        "random_seed": args.seed,
        "structuring_scenarios":args.structuring_scenarios,
        "geo_anomaly_scenarios":args.geo_anomaly_scenarios,
    }
    provided = {name: value for name, value in override.items() if value is not None}

    return replace(GeneratorConfig.from_env(),**provided)


#main function that ties command line together

def main (argv: Sequence[str] | None = None ) -> int:
    args = _build_parser().parse_args(argv)
    #ags now hold the squence of the cli commands of proper data types 
    #parse_args reposnible for conversion of said data types

    logging.basicConfig(level= args.log_level,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    #set up logging info based on the cli commands
    
    #life cycle of the cli commands
    try:
        settings = _reslove_settings(args) #used to combine the cli and environment varribales

    except ValueError as error:
        logger.error("Invalid configuration %s",error)
        return EXIT_INVALID_CONFIG

    logger.info (
        "Replaying to %s to topic '%s' on %s at %sx speed",
        settings.csv_path,
        settings.kafka_topic,
        settings.kafka_bootstrap_servers,
        settings.replay_speedup,
    ) #used to log if the configuration message

    try:
        stats = replay(settings,max_events=args.max_events)
    except KeyboardInterrupt: #enable manula cancelations 
        logger.warning("Interupted queued message were flashed before exit")
        return EXIT_INTERRUPTED
    except (RuntimeError,OSError) as error: #catch kafka error and network flactuations
        logger.error("Replay failed : %s",error)
        return EXIT_REPLAY_FAILED
    #used to publish the success message
    logger.info(
        "Done: %d events published, %d labelled, in %.1f s. Labels: %s",
        stats.published,
        stats.labelled,
        stats.elapsed_seconds,
        settings.ground_truth_path,
    )
    return EXIT_OK

if __name__ == "__main__":
    sys.exit(main())




    