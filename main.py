from app import create_app

# Importar la función create_app desde el módulo app
app = create_app()

if __name__ == '__main__':
    app.run(debug=True)
