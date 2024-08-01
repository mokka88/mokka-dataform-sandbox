module.exports = (
    tags = [],
    table,
    filter = "TRUE",
) => {

    let result = {}

    Object.keys(tenants.tenant2dataset_map).forEach((tenant) => {
        r = publish(table, {
            type: 'table',
            database: constants.target_project,
            schema: tenants.tenant2dataset_map[tenant],
            tags: tags
        }).query(
            (ctx) =>
            `SELECT * EXCEPT(tenant) 
             FROM ${ctx.ref({database: constants.stage_project, schema: constants.stage_dataset, name: "tenant_data"})}
                WHERE 
                    tenant = '${tenant}'
                    AND ${filter}`
        )

        result = {
            ...result,
            r
        }
    });

    return result;
}
