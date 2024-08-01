module.exports = (
    tags = [],
    table,
    filter = "TRUE",
) => {

    let result = {}

    Object.keys(constants.tenant2dataset_map).forEach((tenant) => {
        r = publish(`${table}_${tenant}`, {
            type: 'incremental',
            schema: "sw_stage",
            tags: tags
        }).query(
            (ctx) =>
            `SELECT * EXCEPT(tenant)
                FROM ${ctx.ref("src1")}
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
