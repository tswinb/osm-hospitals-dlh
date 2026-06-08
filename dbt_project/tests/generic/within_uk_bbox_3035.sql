{% test within_uk_bbox_3035(model, column_name) %}

    with
        validation as (select {{ column_name }} as point from {{ model }}),

        validation_errors as (

            select point

            from validation
            -- if this is true, then point is actually outside the bbox
            -- (-14.582675,49.037868,5.419627,61.015725)
            where
                not st_contains(
                    st_transform(
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
                        'EPSG:4326',
                        'EPSG:3035',
                        true
                    ),
                    point
                )
        )

    select *
    from validation_errors

{% endtest %}
