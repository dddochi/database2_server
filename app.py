from flask import Flask, request, jsonify
import test
app = Flask(__name__)
@app.route('/')
def home():
    return 'Hello, World!'
@app.route('/hello_world') #test api
def hello_world():
    return 'Hello, World!'
@app.route('/echo_call/<param>') #get echo api
def get_echo_call(param):
    return jsonify({"param": param})

@app.route('/movie', methods = ['POST'])
def movie():
    data = request.get_json()
    print(data)
    print(data['movie'])
    movie_info = test.movie_info_provider(data['movie'])

    return jsonify(movie_info)
@app.route('/genre', methods = ['POST'])
def genre():
    data = request.get_json()
    print(data)
    print(data['genre'])
    top_list = test.genre_top_5_provider(data['genre'])

    return top_list
@app.route('/recommendation', methods = ['POST'])
def recommendation():
    data = request.get_json()
    print(data)
    print(data['user_id'])
    recommendation_list = test.user_based_recommendation_provider(data['user_id'])

    return recommendation_list

if __name__ == '__main__':
    app.run(debug=True)
# @app.route('/hello_world') #test api
# def hello_world():
#     return 'Hello, World!'
#
# # @app.route('/echo_call/<param>') #get echo api
# # def get_echo_call(param):
# #     return jsonify({"param": param})
# #
# # @app.route('/echo_call', methods=['POST']) #post echo api
# # def post_echo_call():
# #     param = request.get_json()
# #     return jsonify(param)
#
# if __name__ == "__main__":
#     app.run(debug=True)
