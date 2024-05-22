#include "APServer.h"
#define CH_BENCHMARK
using namespace std;
DeltaManager *APServer::deltaManager;
//std::map<int, std::map<string, enum Status>> APServer::orderStatus;
std::map<int, std::map<string, Graph<ulong>>>
    APServer::orderMap;
std::map<int, std::map<string, std::map<ulong, vector<state_transfer::Update>>>>
    APServer::updateMap;
std::map<int, std::map<string, std::map<ulong, odVertex>>> APServer::orderVec;
std::mutex APServer::mtxUpMap[30][40]; //hardcode
std::mutex APServer::mtxOdMap[30][40]; //hardcode
std::mutex batchMtx;
std::recursive_mutex mtx_ = {};
std::map<ulong, std::map<int, std::map<string, int>>> APServer::waitlist_;
std::mutex APServer::mu_waitlist_;

void APServer::query()
{
    int count = 0;
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        int base = 610000 + count * 10000;
        Graph<ulong> query_dep_graph;
#if defined CH_BENCHMARK
        int max_query_column = 8;
#endif

        for (int i = 1; i < max_query_column; i++)
        {
            //build the graph to explore
            if (query_touched_col_map.find(base + i * 1000) != query_touched_col_map.end())
            {
                auto col_vec = query_touched_col_map[base + i * 1000];
                for (auto col_id : col_vec)
                {
                    for (int shard_id = 0; shard_id < MAX_SHARD_NUM; shard_id++)
                    {
                        int col_index = tp_info_.ColIDIndex[col_id];
                        mtxUpMap[shard_id][col_index].lock();
                        auto query_per_col_graph = orderMap[shard_id][col_id].getGraph();
                        for (auto it : query_per_col_graph)
                        {
                            if (orderVec[shard_id][col_id].find(it.first) == orderVec[shard_id][col_id].end())
                            {
                                continue;
                            }
                            if (orderVec[shard_id][col_id][it.first].status == 0)
                            //TODO 100
                            {
                                mu_waitlist_.lock();
                                if (waitlist_.find(it.first) == waitlist_.end())
                                {
                                    std::map<int, std::map<string, int>> shard_pair;
                                    std::map<string, int> col_pair;
                                    col_pair.insert(std::make_pair(col_id, 0));
                                    shard_pair.insert(std::make_pair(shard_id, col_pair));
                                    waitlist_.insert(std::make_pair(it.first, shard_pair));
                                }
                                else if (waitlist_[it.first].find(shard_id) == waitlist_[it.first].end())
                                {
                                    std::map<string, int> col_pair;
                                    col_pair.insert(std::make_pair(col_id, 0));
                                    waitlist_[it.first].insert(std::make_pair(shard_id, col_pair));
                                }
                                else
                                {
                                    waitlist_[it.first][shard_id].insert(std::make_pair(col_id, 0));
                                }
                                mu_waitlist_.unlock();
                            }
                        }
                        query_dep_graph.union_(orderMap[shard_id][col_id]);
                        mtxUpMap[shard_id][col_index].unlock();
                    }
                }
            }
        }
        int flag = 0;
        while (true)
        {
            flag++;
            usleep(10000);
            TriggerCleanWaitlist();
            mu_waitlist_.lock();
            if (waitlist_.empty())
            {
                mu_waitlist_.unlock();
                break;
            }
            mu_waitlist_.unlock();
        }
        run_query(count % 22);
        count++;
    }
}
void APServer::TriggerCleanWaitlist()
{
    mu_waitlist_.lock();
    std::map<ulong, std::map<int, std::map<string, int>>> waitlist = waitlist_;
    mu_waitlist_.unlock();
    for (auto txn = waitlist_.begin(); txn != waitlist_.end();)
    {
        for (auto txn_s = txn->second.begin(); txn_s != txn->second.end();)
        {
            for (auto txn_c = txn_s->second.begin(); txn_c != txn_s->second.end();)
            {
                int col_index = tp_info_.ColIDIndex[txn_c->first];
                mtxUpMap[txn_s->first][col_index].lock();
                vector<ulong> visited;
                ApplyUpdateByOrder(txn_s->first, txn_c->first, txn->first, visited);
                orderVec[txn_s->first][txn_c->first][txn->first].status = 1;
                mtxUpMap[txn_s->first][col_index].unlock();
                txn_s->second.erase(txn_c++);
            }
            if (txn_s->second.size() == 0)
                txn->second.erase(txn_s++);
            else
                txn_s++;
        }
        if (txn->second.size() == 0)
            waitlist_.erase(txn++);
        else
            txn++;
    }
}
grpc::Status APServer::TransferUpdate(grpc::ServerContext *context,
                                      grpc::ServerReaderWriter<state_transfer::UpdateResp,
                                                               state_transfer::UpdateReq> *stream)
{
    state_transfer::UpdateReq up;
    int batch_id = 1;
    std::mutex timeMtx;
    while (stream->Read(&up))
    {
        if (batch_id == 1)
        {
            timeMtx.lock();
            if (!DeltaManager::ts.started)
            {
                std::thread(&APServer::query, this).detach();
                DeltaManager::ts.start();
            }
            timeMtx.unlock();
        }

        std::vector<std::future<void>> results;
        for (int i = 0; i < up.updates().size(); i++)
        {
            int col_index = tp_info_.ColIDIndex[up.col_id()];
            mtxUpMap[up.shard_id()][col_index].lock();
            if (up.updates(i).op_type() == 0)
            {
                //Transactions may have multiple updates in the same shard and column
                if (updateMap[up.shard_id()][up.col_id()].find(up.updates(i).txnid()) !=
                    updateMap[up.shard_id()][up.col_id()].end())
                {
                    //updateMap[up.shard_id()][up.col_id()][up.updates(i).txnid()].push_back(up.updates(i));
                }
                else
                {
                    vector<state_transfer::Update> optVec;
                    optVec.push_back(up.updates(i));
                    updateMap[up.shard_id()][up.col_id()].insert(std::make_pair(up.updates(i).txnid(), optVec));
                }
                mtxUpMap[up.shard_id()][col_index].unlock();
                backUpdate(up.shard_id(), up.col_id(), up.updates(i));
            }
            else if (up.updates(i).op_type() == 1)
            {
                if (updateMap[up.shard_id()][up.col_id()].find(up.updates(i).txnid()) !=
                    updateMap[up.shard_id()][up.col_id()].end())
                {
                    //updateMap[up.shard_id()][up.col_id()][up.updates(i).txnid()].push_back(up.updates(i));
                }
                else
                {
                    vector<state_transfer::Update> optVec;
                    optVec.push_back(up.updates(i));
                    updateMap[up.shard_id()][up.col_id()].insert(std::make_pair(up.updates(i).txnid(), optVec));
                }
                mtxUpMap[up.shard_id()][col_index].unlock();
                backUpdate(up.shard_id(), up.col_id(), up.updates(i));
            }
            else
            {
                mtxUpMap[up.shard_id()][col_index].unlock();
            }
        }
        state_transfer::UpdateResp resp;
        resp.set_res(1);
        stream->Write(resp);
        batch_id++;
    }
    return grpc::Status::OK;
}

grpc::Status APServer::TransferOrder(grpc::ServerContext *context,
                                     grpc::ServerReaderWriter<state_transfer::OrderResp,
                                                              state_transfer::OrderReq> *stream)
{
    state_transfer::OrderReq od;
    while (stream->Read(&od))
    {
        /**
         * For Debug:
         * printf("Received %d orders from shard%d\n", od.orders().size(), od.shard_id());
         * **/
        std::vector<std::future<void>> results;
        for (int i = 0; i < od.orders().size(); i++)
        {
            for (int j = 0; j < od.orders(i).touched_cols().size(); j++)
            {
                int col_index = tp_info_.ColIDIndex[od.orders(i).touched_cols(j).col_id()];
                mtxUpMap[od.shard_id()][col_index].lock();
                orderMap[od.shard_id()][od.orders(i).touched_cols(j).col_id()].add_vertex(od.orders(i).txn_id());
                odVertex odv;
                odv.status = 0;
                odv.estimated_time = od.orders(i).estimated_lat();
                // std::cout << "verify estimated time: " << odv.estimated_time << "now: " << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()
                //           << "time gap: " << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count() - odv.estimated_time << std::endl;
                odv.up_cnt = 1; //FIXME
                orderVec[od.shard_id()][od.orders(i).touched_cols(j).col_id()].insert(std::make_pair(od.orders(i).txn_id(), odv));
                for (int k = 0; k < od.orders(i).parents_tid().size(); k++)
                {
                    // if (orderMap[od.shard_id()][od.orders(i).touched_cols(j).col_id()].isExist(od.orders(i).parents_tid(k)) &&
                    //     orderVec[od.shard_id()][od.orders(i).touched_cols(j).col_id()][od.orders(i).parents_tid(k)].status == 0)
                    // {
                    orderMap[od.shard_id()][od.orders(i).touched_cols(j).col_id()].add_vertex(od.orders(i).parents_tid(k));
                    odVertex podv;
                    podv.status = 0;
                    podv.estimated_time = 0;
                    podv.up_cnt = 1;
                    orderVec[od.shard_id()][od.orders(i).touched_cols(j).col_id()].insert(std::make_pair(od.orders(i).parents_tid(k), podv));
                    // if (!orderMap[od.shard_id()][od.orders(i).touched_cols(j).col_id()].parentExist(od.orders(i).txn_id(), od.orders(i).parents_tid(k)))
                    // {
                    orderMap[od.shard_id()][od.orders(i).touched_cols(j).col_id()].add_edge(od.orders(i).parents_tid(k), od.orders(i).txn_id());
                    // }
                    // }
                }
                mtxUpMap[od.shard_id()][col_index].unlock();
                backOrder(od.shard_id(), od.orders(i).touched_cols(j).col_id(), od.orders(i));
            }
        }
        state_transfer::OrderResp resp;
        resp.set_res(1);
        stream->Write(resp);
    }
    return grpc::Status::OK;
}
bool APServer::TriggerCheckParent(int shard_id, string col_id, ulong tid, vector<ulong> &visited)
{
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    if (orderMap[shard_id][col_id].get_parents(tid).size() == 0)
    {
        //no dep find, apply directly
        for (auto &up : updateMap[shard_id][col_id][tid])
        {
            timer t;
            t.start();
            deltaManager->ingestup(shard_id, col_id, up, t);
        }
        mu_waitlist_.lock();
        if (waitlist_.find(tid) != waitlist_.end() && waitlist_[tid].find(shard_id) != waitlist_[tid].end() && waitlist_[tid][shard_id].find(col_id) != waitlist_[tid][shard_id].end())
        {
            waitlist_[tid][shard_id][col_id] = 1;
        }
        mu_waitlist_.unlock();
        orderVec[shard_id][col_id][tid].status = 1;
        //updateMap[shard_id][col_id].erase(tid);
        return true;
    }
    bool dep_check = true;
    auto dep = orderMap[shard_id][col_id].get_parents(tid);
    for (auto it = dep.begin(); it != dep.end(); it++)
    {
        if (orderVec[shard_id][col_id][*it].status == 0)
        {
            if (!ApplyUpdateByOrder(shard_id, col_id, *it, visited))
            {
                dep_check = false;
                break;
            }
        }
    }
    if (dep_check == true)
    {
        //all dep applied
        for (auto &up : updateMap[shard_id][col_id][tid])
        {
            timer t;
            t.start();
            deltaManager->ingestup(shard_id, col_id, up, t);
        }
        mu_waitlist_.lock();
        if (waitlist_.find(tid) != waitlist_.end() && waitlist_[tid].find(shard_id) != waitlist_[tid].end() && waitlist_[tid][shard_id].find(col_id) != waitlist_[tid][shard_id].end())
        {
            waitlist_[tid][shard_id][col_id] = 1;
        }
        mu_waitlist_.unlock();
        orderVec[shard_id][col_id][tid].status = 1;
        updateMap[shard_id][col_id].erase(tid);
        return true;
    }
    return false;
}

bool APServer::ApplyUpdateByOrder(int shard_id, string col_id, ulong tid, vector<ulong> &visited)
{

    std::lock_guard<std::recursive_mutex> lock(mtx_);
    if (orderVec[shard_id][col_id][tid].status == 1)
    {
        return true;
    }
    if (find(visited.begin(), visited.end(), tid) != visited.end())
    {
        orderVec[shard_id][col_id][tid].status = 1;
        return true;
    }
    else
    {
        visited.push_back(tid);
    }
    //check updates to execute, call update waitlist
    if (!orderMap[shard_id][col_id].isExist(tid) || updateMap[shard_id][col_id].find(tid) == updateMap[shard_id][col_id].end())
    {
        //can not find order/update so far
        return false;
    }
    /**
     * For Debug:
     * std::cout << " tid: " << tid << " col_id: " << col_id << " expected up num: " << orderVec[shard_id][col_id][tid].up_cnt << " received up num " << updateMap[shard_id][col_id][tid].size() << std::endl;
    **/
    // std::cout << " tid: " << tid << " col_id: " << col_id << " expected up num: " << orderVec[shard_id][col_id][tid].up_cnt << " received up num " << updateMap[shard_id][col_id][tid].size() << std::endl;
    if (orderVec[shard_id][col_id][tid].up_cnt != updateMap[shard_id][col_id][tid].size())
    {
        //do not receive all ups for the txn in the same col
        return false;
    }
    //check whether all dep has applied
    return TriggerCheckParent(shard_id, col_id, tid, visited);
}

void APServer::backUpdate(int shard_id, string col_id, state_transfer::Update up)
{
    int col_index = tp_info_.ColIDIndex[col_id];
    mtxUpMap[shard_id][col_index].lock();
    vector<ulong> visited;
    ApplyUpdateByOrder(shard_id, col_id, up.txnid(), visited);
    mtxUpMap[shard_id][col_index].unlock();
}

void APServer::backOrder(int shard_id, string col_id, state_transfer::Order od)
{
    int col_index = tp_info_.ColIDIndex[col_id];
    mtxUpMap[shard_id][col_index].lock();
    vector<ulong> visited;
    ApplyUpdateByOrder(shard_id, col_id, od.txn_id(), visited);
    mtxUpMap[shard_id][col_index].unlock();
}

void APServer::printUpMap()
{
    sleep(60);
    std::ofstream outfile;
    outfile.open("updateMap.txt");
    for (auto iter = updateMap.begin(); iter != updateMap.end(); iter++)
    {
        outfile << "shard_id: " << iter->first << " : " << std::endl;
        for (auto iter1 = iter->second.begin(); iter1 != iter->second.end(); iter1++)
        {
            outfile << "          ==========col_id: " << iter1->first << " : " << std::endl;
            for (auto iter2 = iter1->second.begin(); iter2 != iter1->second.end(); iter2++)
            {
                outfile << "                    ==========tid: " << iter2->first << " : " << std::endl;
                for (auto u : iter2->second)
                {
                    outfile << "                                       =========up" << std::endl;
                }
            }
        }
    }
    outfile.close();
}

void APServer::printOdMap()
{
    sleep(60);
    std::ofstream outfile;
    outfile.open("OrderMap.txt");
    for (auto iter = orderMap.begin(); iter != orderMap.end(); iter++)
    {
        outfile << "shard_id: " << iter->first << " : " << std::endl;
        for (auto iter1 = iter->second.begin(); iter1 != iter->second.end(); iter1++)
        {
            outfile << "          ==========col_id: " << iter1->first << " : " << std::endl;
            auto edges = iter1->second.getGraph();
            for (auto iter2 = edges.begin(); iter2 != edges.end(); iter2++)
            {
                // outfile << "                    ==========tid: " << iter2->first << " : " << std::endl;
                for (auto iter3 = iter2->second.parents.begin(); iter3 != iter2->second.parents.end(); iter3++)
                {
                    outfile << "g.add_edge(" << iter2->first << "," << *iter3 << ");" << std::endl;
                    // outfile << "                              ==========ptid: " << *iter3 << " : " << std::endl;
                }
            }
            iter1->second.search_circle();
        }
    }
    outfile << deltaManager->cnt;
    outfile.close();
}

void RunServer()
{
    importCSV("/htap/csv/");
    //DeltaManager::ts.start();
    std::thread(&statistic, 10).detach();
    DeltaManager deltaManager;
    threadParameters prm;
    pthread_barrier_t barStart;
    prm.barStart = &barStart;
    ThreadPool threadPool(Config::getLogWorkerNum(), &prm);
    APServer service(&deltaManager, &threadPool);
    std::string server_address(service.get_server_address_());
    grpc::ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main()
{
    RunServer();

    return 0;
}
