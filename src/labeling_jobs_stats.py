import os
import supervisely_lib as sly
from supervisely_lib.labeling_jobs.utils import is_completed, is_stopped, is_not_started, is_on_labeling, is_on_review, \
    is_review_started, is_labeling_started


my_app = sly.AppService()

TEAM_ID = int(os.environ['context.teamId'])

def count_jobs_statuses(jobs):
    columns = ["job status", "quantity", "percentage", "description"]

    count_total = len(jobs)
    count_total_name = "TOTAL"
    count_total_desc = "the total number of jobs in current team"

    count_stopped = 0
    count_stopped_name = "STOPPED"
    count_stopped_desc = "stopped jobs"

    count_completed = 0
    count_completed_name = "COMPLETED"
    count_completed_desc = "completed jobs"

    count_pending = 0
    count_pending_name = "PENDING"
    count_pending_desc = "job is created but not started"

    count_labeling_zero = 0
    count_labeling_zero_name = "IN PROGRESS BUT NOTHING IS LABELED"
    count_labeling_zero_desc = "job is started but no activity yet"

    count_labeling_started = 0
    count_labeling_started_name = "IN PROGRESS WITH LABELS"
    count_labeling_started_desc = "at least one image is marked as finished"

    count_labeling_ip = 0
    count_labeling_ip_name = "IN PROGRESS"
    count_labeling_ip_desc = "IN PROGRESS BUT NOTHING IS LABELED + IN PROGRESS WITH LABELS"

    count_review_zero = 0
    count_review_zero_name = "ON REVIEW BUT NOTHING IS REVIEWED"
    count_review_zero_desc = "no review activity yet"

    count_review_ip = 0
    count_review_ip_name = "ON REVIEW WITH FINISHED IMAGES"
    count_review_ip_desc = "no review activity yet"



    #count_labeling_not_started = 0  # pending

    count_on_labeling = 0
    count_labeling_started = 0
    count_labeling_zero_done = 0

    count_on_review = 0
    count_review_started = 0
    count_review_zero_done = 0

    for job in jobs:
        if is_completed(job):
            count_completed += 1
        elif is_stopped(job):
            count_stopped += 1
        elif is_not_started(job):
            count_labeling_not_started += 1
        elif is_on_labeling(job):
            count_on_labeling += 1
            if is_labeling_started(job):
                count_labeling_started += 1
        elif is_on_review(job):
            count_on_review += 1
            if is_review_started(job):
                count_review_started += 1
        else:
            raise RuntimeError("Unhandled job status: {}".format(str(job)))

    count_labeling_zero_done = count_on_labeling - count_labeling_started
    count_review_zero_done = count_on_review - count_review_started

@my_app.callback("preprocessing")
@sly.timeit
def preprocessing(api: sly.Api, task_id, context, state, app_logger):
    team = api.team.get_info_by_id(TEAM_ID)
    jobs = api.labeling_job.get_list(team.id)
    stats = [api.labeling_job.get_stats(job.id) for job in jobs]

    #df_jobs = sly.lj.jobs_stats(os.environ['SERVER_ADDRESS'], jobs, stats)
    df_jobs_summary = sly.lj.jobs_summary(jobs)
    #df_images_summary = sly.lj.images_summary(jobs)
    #df_classes_summary = sly.lj.classes_summary(stats)
    #df_tags_summary = sly.lj.tags_summary(stats)



    my_app.stop()

def main():
    data = {
    }

    state = {
    }

    initial_events = [{"state": None, "context": None, "command": "preprocessing"}]

    # Run application service
    my_app.run(data=data, state=state, initial_events=initial_events)


#@TODO - handle case with zero jobs
if __name__ == "__main__":
    sly.main_wrapper("main", main)