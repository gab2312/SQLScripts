CREATE procedure dw.SVT_dim_clientDataHistoric_Load (@sourceTable varchar(50), @destinationTable varchar(50), @field varchar(30))
as

DECLARE @CurrentDateTime varchar(10);
DECLARE @MinDateTime varchar(10);
DECLARE @MaxDateTime varchar(10);

SELECT
	@CurrentDateTime = cast(getdate() as date),
	@MinDateTime = cast('1900-01-01' as date),
	@MaxDateTime = cast('9999-12-31' as date);

declare @query nvarchar(max) =
                                'INSERT INTO dw.'+ @destinationTable +'
                                (
                                    idClient,
                                    '+ @field +',
                                    validFrom,
                                    validUp
                                )
                                SELECT
                                    idClient,
                                    '+ @field +',
                                    validFrom,
                                    validUp
                                FROM
                                (
                                    MERGE dw.'+ @destinationTable +' as Target
                                    USING
                                    (
                                            select b.idClient, '+ @field +'_OLD, '+ @field +'_NEW
                                            from stg.'+ @sourceTable +' a
                                                     inner join dw.dim_clients b on a.idSeleccionado = b.idMcc
                                            where mergeAction = ''UPDATE''
                                              and a.origenSeleccionado = ''MCC''
                                              and '+ @field +'_OLD <> '+ @field +'_NEW
                                            union
                                            select b.idClient, '+ @field +'_OLD, '+ @field +'_NEW
                                            from stg.'+ @sourceTable +' a
                                                     inner join dw.dim_clients b on a.idSeleccionado = b.idCmp
                                            where mergeAction = ''UPDATE''
                                              and a.origenSeleccionado = ''CMP''
                                              and '+ @field +'_OLD <> '+ @field +'_NEW
                                    ) as Source
                                    ON
                                    (
                                        Source.idClient = Target.idClient
                                    )
                                    WHEN NOT MATCHED BY TARGET
                                    THEN INSERT
                                    (
                                        idClient,
                                        '+ @field +',
                                        validFrom,
                                        validUp
                                    )
                                    VALUES
                                    (
                                        Source.idClient,
                                        Source.'+ @field +'_NEW,
                                        cast('''+ @MinDateTime + ''' as date),
                                        cast('''+ @MaxDateTime +''' as date)
                                    )
                                WHEN MATCHED AND
                                (
                                    (cast(validUp as date) = cast('''+ @MaxDateTime +''' as date) OR (validUp IS NULL AND cast('''+ @MaxDateTime +''' as date) IS NULL))
                                )
                                AND
                                (
                                    (Target.'+ @field +' <> Source.'+ @field +'_NEW OR (Target.'+ @field +' IS NULL AND Source.'+ @field +'_NEW IS NOT NULL) OR (Target.'+ @field +' IS NOT NULL AND Source.'+ @field +'_NEW IS NULL))
                                )
                                    THEN UPDATE
                                    SET
                                        validUp = dateadd(day, -1, cast(''' + @CurrentDateTime +''' as date))
                                    OUTPUT
                                        $Action as [MERGE_ACTION],
                                        Source.idClient AS idClient,
                                        Source.'+ @field +'_NEW AS '+ @field +',
                                        cast('''+ @CurrentDateTime +''' as date) AS validFrom,
                                        cast('''+ @MaxDateTime +''' as date) AS validUp

                                ) MERGE_OUTPUT
                                WHERE MERGE_OUTPUT.[MERGE_ACTION] = ''UPDATE'' AND MERGE_OUTPUT.idClient IS NOT NULL;';

EXEC sp_executesql @query;
go

