import pyspark.sql.functions as F

from pyspark.sql import Window


def get_consistency_check_quantities(self, enrollments, df):
    """Check that the enrollments view is consistent with the data view

    i.e.:
    - no missing client_ids from either
    - check for duplicate client_id values
    """
    return dict(
        dupe_client_id_enrollments=enrollments.filter(
            F.count(enrollments).over(Window.partitionBy(enrollments.client_id)) > 1
        ),

        missing_enrollments=df.filter(
            df.submission_date_s3 >= self.start_date
        ).filter(
            ~F.isnull(df.experiments[self.experiment_slug])
        ).select(
            df.client_id, df.experiments[self.experiment_slug]
        ).distinct(
        ).join(
            enrollments,
            [
                enrollments.client_id == df.client_id,
                enrollments.branch == df.experiments[self.experiment_slug],

                # TODO accuracy: once we can rely on
                #   `df.experiments[self.experiment_slug]`
                # existing even after unenrollment, we could start joining on
                # branch to reduce problems associated with split client_ids.

                # Do a quick pass aiming to efficiently filter out lots of rows:
                enrollments.enrollment_date <= df.submission_date_s3,

            ],
            how='left'
        ).filter(
            F.isnull(enrollments.enrollment_date)
        ).select(
            df.client_id,
            df.submission_date_s3,
            df.experiments[self.experiment_slug],
        ),
    )
