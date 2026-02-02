import dagster as dg


def create_deduplicated_asset_job_schedule(
    name: str,
    # TODO: somehow make this type-able, dagster doesn't really export the type to do
    # this properly, check out the `job` params of @dg.schedule
    job,
    cron_schedule: str,
    execution_timezone: str,
    default_status: dg.DefaultScheduleStatus = dg.DefaultScheduleStatus.RUNNING,
):
    """
    Create a job schedule that will not run if there's job is already running.

    Ref: https://github.com/dagster-io/dagster/discussions/8414#discussioncomment-2958756

    Params:
    name: the name (also identifier) for the job that you want to create
    job: the job you want to create the schedule for. Could be
         "UnresolvedAssetJobDefinition" as returned by `dg.define_asset_job`
    cron_schedule: the run schedule you want the schedule to run on
    execution_timezone: the timezone you want your cron to be evaluated at
    default_status: default status of the schedule to run or not
    """

    @dg.schedule(
        name=name,
        job=job,
        cron_schedule=cron_schedule,
        execution_timezone=execution_timezone,
        default_status=default_status,
    )
    def schedule_runner(context: dg.ScheduleEvaluationContext):
        run_records = context.instance.get_run_records(
            dg.RunsFilter(job_name=job.name, statuses=[dg.DagsterRunStatus.STARTED])
        )

        if len(run_records) == 0:
            context.log.info(f"There's no running job of '{job.name}' starting one...")
            yield dg.RunRequest()
        else:
            context.log.warning(
                f"There's already >0 run of the job '{job.name}', ignoring!"
            )

    return schedule_runner
