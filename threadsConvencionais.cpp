#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <thread>
#include <map>
#include <algorithm>
#include <array>
#include <chrono>

#define TAMANHO_LOTE 30000 //30000

struct colunas {
    std::string idatracacao;
    std::string cdtup;
    std::string berco;
    std::string portoatracacao;
    std::string ano;
    std::string mes;
    std::string tipooperacao;
    std::string tiponavegacaoatracacao;
    std::string terminal;
    std::string nacionalidadearmador;
    std::string tesperaatracacao;
    std::string tesperacainicioop;
    std::string toperacao;
    std::string tesperadesatracacao;
    std::string tatracado;
    std::string testadia;
    std::string idcarga;
    std::string origem;
    std::string destino;
    std::string cdmercadoria;
    std::string naturezacarga;
    std::string qtcarga;
    std::string pesocargabruta;
    std::string sentido;
    std::string cdmercadoria_cntr;
    std::string pesocarga_cntr;
};

void removeLineBreak(std::string& str){
    str.erase(std::remove(str.begin(), str.end(), '\n'), str.end());
    str.erase(std::remove(str.begin(), str.end(), '\r'), str.end());
}

//Parseia o csv e retorna os dados para o vetor de registros
std::vector<colunas> parseCSV(std::ifstream &file, unsigned int qtd_linhas_lote) {
    std::vector<colunas> registros;
    std::string line;

    unsigned int linhas_lidas = 0;

    while (linhas_lidas < qtd_linhas_lote && std::getline(file, line)) {

        std::istringstream sstream(line);
        colunas registro;
        std::string temp;

        getline(sstream, registro.idatracacao, ',');
        getline(sstream, registro.cdtup, ',');
        getline(sstream, registro.berco, ',');
        getline(sstream, registro.portoatracacao, ',');
        getline(sstream, registro.ano, ',');
        getline(sstream, registro.mes, ',');
        getline(sstream, registro.tipooperacao, ',');
        getline(sstream, registro.tiponavegacaoatracacao, ',');
        getline(sstream, registro.terminal, ',');
        getline(sstream, registro.nacionalidadearmador, ',');
        getline(sstream, registro.tesperaatracacao, ',');
        getline(sstream, registro.tesperacainicioop, ',');
        getline(sstream, registro.toperacao, ',');
        getline(sstream, registro.tesperadesatracacao, ',');
        getline(sstream, registro.tatracado, ',');
        getline(sstream, registro.testadia, ',');
        getline(sstream, registro.idcarga, ',');
        getline(sstream, registro.origem, ',');
        getline(sstream, registro.destino, ',');
        getline(sstream, registro.cdmercadoria, ',');
        getline(sstream, registro.naturezacarga, ',');
        getline(sstream, registro.qtcarga, ',');
        getline(sstream, registro.pesocargabruta, ',');
        getline(sstream, registro.sentido, ',');
        getline(sstream, registro.cdmercadoria_cntr , ',');
        getline(sstream, registro.pesocarga_cntr, ',');
        removeLineBreak(registro.pesocarga_cntr);

        registros.push_back(registro);
        ++linhas_lidas;
    }

    return registros;
}

//passa int para string
const std::string int2str(int i){
    std::ostringstream s;
    s << i;
    return s.str();
}

//Processa a coluna e faz a codificacao
void processColuna(std::vector<colunas>& registros, std::string& coluna, std::map<std::string, std::string>& ft1) {

    int i = 0;
    std::string* conteudo;

    for (auto& registro : registros) {
        if (coluna == "cdtup") conteudo = &registro.cdtup;
        else if (coluna == "berco") conteudo = &registro.berco;
        else if (coluna == "portoatracacao") conteudo = &registro.portoatracacao;
        else if (coluna == "mes") conteudo = &registro.mes;
        else if (coluna == "tipooperacao") conteudo = &registro.tipooperacao;
        else if (coluna == "tiponavegacaoatracacao") conteudo = &registro.tiponavegacaoatracacao;
        else if (coluna == "terminal") conteudo = &registro.terminal;
        else if (coluna == "origem") conteudo = &registro.origem;
        else if (coluna == "destino") conteudo = &registro.destino;
        else if (coluna == "cdmercadoria") conteudo = &registro.cdmercadoria;
        else if (coluna == "naturezacarga") conteudo = &registro.naturezacarga;
        else if (coluna == "sentido") conteudo = &registro.sentido;

        if (ft1.find(*conteudo) == ft1.end()) {
            std::string indice = int2str(ft1.size() + 1);
            ft1[*conteudo] = indice;
            *conteudo = indice;
        }
        else{
            *conteudo = ft1[*conteudo];
        }

        i++;
    }

}

//Escreve os mini datasets
void geraMiniDataset(std::string nomeColuna, std::map<std::string, std::string>& ft1) {
        std::ofstream myfile;
        myfile.open(("coluna_" + nomeColuna + ".csv").c_str(), std::ios::out);

        std::string key = "";
        auto it = ft1.find(key);
        if (it != ft1.end()) {
            ft1.erase(it);
        }

        myfile << "CODIGO, VALOR" << std::endl;

        for (const auto& pair : ft1) {
            myfile << pair.second << "," << pair.first << std::endl;
        }
        myfile.close();

}

//separa o conteudo do Header em um vector
std::vector<std::string> splitString(const std::string& input, char delimiter) {
    std::vector<std::string> tokens;
    std::stringstream ss(input);
    std::string token;

    while (std::getline(ss, token, delimiter)) {
        tokens.push_back(token);
    }

    return tokens;
}

//Checa se a string ta no vector
bool isStringInVector(const std::vector<std::string>& vec, const std::string& str) {
    return std::find(vec.begin(), vec.end(), str) != vec.end();
}


int main() {
    const auto start{std::chrono::steady_clock::now()};
    const std::string filename = "dataset_mil.csv";

    //especifica as colunas que vao ser codificadas
    std::vector<std::string> colunasVector;
    colunasVector = { "cdtup", "berco", "portoatracacao", "mes", "tipooperacao", "tiponavegacaoatracacao", "terminal", "origem", "destino", "cdmercadoria", "naturezacarga", "sentido" };


    //cria um vetor de maps, onde cada map é p uma coluna especifica
    std::vector<std::map<std::string, std::string>> ft1(colunasVector.size());
    unsigned int linha_atual = 0;

    //cria os arquivos dos minicsvs
    for (auto& nomeColuna : colunasVector) {
        std::fstream arquivos;
        arquivos.open(("coluna_" + nomeColuna + ".csv").c_str(), std::ios::in | std::ios::out);
        arquivos.close();
    }

    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Erro ao abrir o arquivo: " << filename << std::endl;
        return 1;
    }
    std::string line;
    if (!std::getline(file, line)) {
        std::cerr << "Erro ao ler o cabeçalho do arquivo: " << filename << std::endl;
        return 1;
    }

    std::string outputFilename = "ds_final_codificado.csv";
    std::ofstream outputFile;

    outputFile.open(outputFilename);
    if (!file.is_open()) {
        std::cerr << "Erro ao abrir o arquivo: " << filename << std::endl;
        return 1;
    }

    //gera cabeçalho do csv final
    //std::cout << line << std::endl;
    removeLineBreak(line);
    std::vector<std::string> header = splitString(line, ',');

    for(int i = 0; i < header.size(); i++){
        if(isStringInVector(colunasVector, header[i])){
            outputFile << "CODIGO_";
        }
        outputFile << header[i];
        if(i < header.size() - 1) outputFile << ",";
    }

    outputFile << std::endl;

    std::cout << "Iniciando escrita CSV codificado..." << std::endl;
    std::cout << "-----------------------------------" << std::endl;

    //loop principal
    while (true) {
        int linha_inicial = linha_atual;
        auto registros = parseCSV(file, TAMANHO_LOTE);
        if (registros.empty()) {
            break;
        }

        std::vector<std::thread> cols_threads(colunasVector.size());

        //cria os maps usando paralelismo
        for (int i = 0; i < colunasVector.size();) {
            cols_threads[i] = std::thread(processColuna, std::ref(registros), std::ref(colunasVector[i]), std::ref(ft1[i]));
            ++i;
        }

        for (auto& thread : cols_threads) {
            thread.join();
        }

        //escreve o dataset final
                for(int i = 0; i < registros.size(); i++){
            colunas registro = registros[i];

            outputFile  << registro.idatracacao << ","
                        << registro.cdtup << ","
                        << registro.berco << ","
                        << registro.portoatracacao << ","
                        << registro.ano << ","
                        << registro.mes << ","
                        << registro.tipooperacao << ","
                        << registro.tiponavegacaoatracacao << ","
                        << registro.terminal << ","
                        << registro.nacionalidadearmador << ","
                        << registro.tesperaatracacao << ","
                        << registro.tesperacainicioop << ","
                        << registro.toperacao << ","
                        << registro.tesperadesatracacao << ","
                        << registro.tatracado << ","
                        << registro.testadia << ","
                        << registro.idcarga << ","
                        << registro.origem << ","
                        << registro.destino << ","
                        << registro.cdmercadoria << ","
                        << registro.naturezacarga << ","
                        << registro.qtcarga << ","
                        << registro.pesocargabruta << ","
                        << registro.sentido << ","
                        << registro.cdmercadoria_cntr << ","
                        << registro.pesocarga_cntr << std::endl;
        }

        linha_atual += TAMANHO_LOTE;
        std::cout << "Lendo linhas " << linha_inicial << "->" << linha_atual << std::endl;
    }

    std::cout << "-----------------------------------" << std::endl;
    std::cout << "Escrita do dataset codificado concluida." << std::endl;
    std::cout << "Escrita mini CSVs iniciando..." << std::endl;

    //escreve os arquivos de forma paralela
    std::vector<std::thread> minids_threads;
    for (int i = 0; i < colunasVector.size(); i++) {
        minids_threads.push_back(std::thread(geraMiniDataset, colunasVector[i], std::ref(ft1[i])));
    }

    for (auto& thread : minids_threads) {
        thread.join();
    }

    std::cout << "Escrita dos mini CSVs concluida." << std::endl;

    file.close();
    outputFile.close();

    const auto end{std::chrono::steady_clock::now()};
    const std::chrono::duration<double> elapsed_seconds{end - start};
    std::cout << "RUNTIME: " << elapsed_seconds.count() << " sec"  << std::endl;

    return 0;
}