import pandas as pd


def rodar(objetos):
    # ---- 1. fazer joins/merges ---- #
    df_merge_orders = pd.merge(objetos["df_orders"]["df"], objetos["df_order_details"]["df"],
                               how="left",
                               on="order_id")
    
    df_orders_w_shippers = pd.merge(df_merge_orders, objetos["df_shippers"]["df"],
                                    how="left",
                                    left_on="ship_via", right_on="shipper_id")
    
    df_orders_w_customers = pd.merge(df_orders_w_shippers, objetos["df_customers"]["df"],
                                     how="left",
                                     on="customer_id")
    
    df_orders_w_employees = pd.merge(df_orders_w_customers, objetos["df_employees"]["df"],
                                     how="left",
                                     on="employee_id")
    
    df_products_w_categories = pd.merge(objetos["df_products"]["df"], objetos["df_categories"]["df"],
                                        how="left",
                                        on="category_id")
    
    df_orders_w_products = pd.merge(df_orders_w_employees, df_products_w_categories,
                                    how="left",
                                    on="product_id")
    
    df_orders_w_suppliers = pd.merge(df_orders_w_products, objetos["df_suppliers"]["df"],
                                     how="left",
                                     on="supplier_id")
    
    # df_orders_w_suppliers.to_excel(r"C:\Users\Felipe\OneDrive\OneDrive\Área de Trabalho\py-gcp\etl\dw\resultado.xlsx", index=False)
    tabelao = df_orders_w_suppliers.copy()

    # ---- 2. ler a matriz para filtrar as colunas ---- #
    df_matriz_colunas = pd.read_excel(r"C:\Users\Felipe\OneDrive\OneDrive\Área de Trabalho\py-gcp\docs\matriz_colunas_do_modelo.xlsx",
                                      sheet_name="Planilha1", engine="openpyxl")
    
    objetos_do_modelo = ["fato_pedidos", "dim_produtos", "dim_categorias",
                         "dim_entregadores", "dim_clientes", "dim_funcionarios",
                         "dim_fornecedores"]
    
    # ---- 3. criar dicionário para armazenar todos os objetos e DataFrames finais ---- #
    dict_objetos_finais = {}

    for item in objetos_do_modelo:
        # ---- 4. filtrar a matriz ---- #
        df_filtrado = df_matriz_colunas[["colunas", "renomear", item]].query(f"{item}=='x'").copy()

        # ---- 5. selecionar as colunas do DataFrame final usando o DataFrame filtrado correspondente ---- #
        cols = df_filtrado["colunas"].tolist()
        df_final = tabelao[cols].copy()

        # ---- 6. renomear colunas se necessário ---- #
        df_rename = df_filtrado[df_filtrado["renomear"].notnull()]

        if df_rename.shape[0] > 0: # verificar se DataFrame está vazio
            df_rename = df_rename[["colunas", "renomear"]].copy()

            dict_rename = df_rename.to_dict(orient="list") # necessário fazer o zip
            dict_rename = {chave: valor for chave, valor in zip(dict_rename["colunas"], dict_rename["renomear"])}

            # print(dict_rename)

            df_final.rename(columns=dict_rename, inplace=True)

        # ---- 7. se for dimensão, remover duplicidade de linhas ---- #
        if "dim" in item:
            df_final = df_final.drop_duplicates()

        # ---- 8. aplicar o obs se for a dim_funcionarios
        if "dim_funcionarios" in item:
            df_final["full_name"] = df_final["first_name"].str.cat(df_final["last_name"], sep=" ")

        # ---- 9. adicionar o objeto e o DataFrame final ao dicionário
        dict_objetos_finais[item] = {"name": item,
                                     "df_final": df_final,
                                     "matriz_filtrada": df_filtrado,
                                     }
        
        # ---- 10. retorno ---- #
        if len(dict_objetos_finais) > 0:
            return {"deve-rodar": True,
                    "dict_objetos_finais": dict_objetos_finais,
                    }
        else:
            return {"deve-rodar": False,}