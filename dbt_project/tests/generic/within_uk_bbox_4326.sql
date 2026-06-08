{% test within_uk_bbox_4326(model, column_name) %}

    with
        validation as (select {{ column_name }} as point from {{ model }}),

        validation_errors as (

            select point

            from validation
            -- if this is true, then point is actually outside the bbox
            -- (-14.582675,49.037868,5.419627,61.015725)
            where
                not st_contains(
                    (
                        select st_extent_agg(geom)
                        from
                            unnest(
                                [
                                    st_point(-14.582675, 49.037868),
                                    st_point(5.419627, 61.015725)
                                ]
                            ) as _(geom)
                    ),
                    point
                )
        )

    select *
    from validation_errors

{% endtest %}
