from pathlib import Path

from src.utils.log import getLogger
log = getLogger('[app]')

try:
    import traceback
    import asyncio
    from pandas import DataFrame
    from config.app_config import OUTPUT_PATH
    from src.components.aws_s3_client import AWS3Client
    from src.components.doc_parser_executor import DocParserExecutor
    from src.utils.benchmark import timeit
    import nest_asyncio

except ModuleNotFoundError as e:
    log.error('Please run "pip install -r requirements.txt" to install dependencies')
    raise


async def main():
    try:
        aws = AWS3Client()
        async with aws.s3_client():
            executor = DocParserExecutor(aws)
            docs = await aws.list_docs()
            credit_card_info = await executor.extract_all_cc_numbers(docs)
            credit_card_info.to_csv(OUTPUT_PATH, index=False)
    except Exception as e:
        log.error(f'got the following error: {e}')
        log.error(traceback.format_exc())
        raise

if __name__ == '__main__':
    nest_asyncio.apply()
    with timeit('end to end'):
        asyncio.get_event_loop().run_until_complete(main())
        msg = f'Done see results in {Path(OUTPUT_PATH).absolute()}'
        log.info(msg)
        print(msg)





